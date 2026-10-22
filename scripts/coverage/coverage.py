"""Package one CI shard, or merge the four reports from the same run/attempt."""

import argparse
import fnmatch
import hashlib
import html
import importlib.metadata
import json
import os
import platform
import re
import shutil
import subprocess
import tomllib
import xml.etree.ElementTree as ET
from pathlib import Path, PurePosixPath

from rust_sources import CoverageError, RustSources, require

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
POLICY = json.loads((HERE / "policy.json").read_text())
JUNIT = ROOT / "target/coverage-nextest/coverage/junit.xml"


def sha(data):
    return hashlib.sha256(data).hexdigest()


def dump(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def command(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()


def fingerprint():
    paths = [HERE / name for name in ("policy.json", "requirements.txt", "rust_sources.py", "coverage.py")]
    paths += [ROOT / ".config/nextest.toml", ROOT / ".github/workflows/ci.yml"]
    return sha(json.dumps({p.relative_to(ROOT).as_posix(): sha(p.read_bytes()) for p in paths},
                          sort_keys=True).encode())


def workspace_packages(root):
    """Include Cargo's automatic in-repository path-dependency members as well."""
    root = root.resolve()
    workspace = tomllib.loads((root / "Cargo.toml").read_text())["workspace"]
    pending = list(workspace["members"])
    packages, manifests = {}, {}
    while pending:
        member = pending.pop()
        require(not any(c in member for c in "*?["), "expand workspace globs explicitly before measuring")
        if member in packages or any(fnmatch.fnmatchcase(member, p) for p in workspace.get("exclude", [])):
            continue
        manifest = root / member / "Cargo.toml"
        data = tomllib.loads(manifest.read_text())
        require("workspace" not in data, f"nested workspace requires explicit ownership: {member}")
        packages[member] = data["package"]["name"]
        manifests[member] = data
        for table in [data] + list(data.get("target", {}).values()):
            for section in ("dependencies", "build-dependencies", "dev-dependencies"):
                for name, spec in table.get(section, {}).items():
                    if not isinstance(spec, dict):
                        continue
                    base = manifest.parent
                    if spec.get("workspace"):
                        spec = workspace["dependencies"][name]
                        base = root
                    if isinstance(spec, dict) and "path" in spec:
                        dependency = (base / spec["path"]).resolve()
                        if dependency.is_relative_to(root):
                            pending.append(dependency.relative_to(root).as_posix())
    return dict(sorted(packages.items())), manifests


def inventory():
    packages, manifests = workspace_packages(ROOT)
    roots = set()
    for member, data in manifests.items():
        # An explicit library path replaces src/lib.rs; it is not an extra root.
        lib = data.get("lib", {})
        if "path" in lib:
            roots.add(f"{member}/{lib['path']}")
        elif (ROOT / member / "src/lib.rs").is_file():
            roots.add(f"{member}/src/lib.rs")
        for target in data.get("bin", []):
            if "path" in target:
                roots.add(f"{member}/{target['path']}")
        if data["package"].get("autobins", True):
            if (ROOT / member / "src/main.rs").is_file():
                roots.add(f"{member}/src/main.rs")
            for pattern in ("*.rs", "*/main.rs"):
                roots.update(p.relative_to(ROOT).as_posix() for p in (ROOT / member / "src/bin").glob(pattern))
    tracked = command("git", "ls-files", "-z").split("\0")
    sources, measured = {}, set()
    for path in tracked:
        if path.endswith(".rs") and any(path.startswith(f"{member}/") for member in packages):
            require(not (ROOT / path).is_symlink(), f"symlinked source: {path}")
            sources[path] = (ROOT / path).read_bytes()
            if any(path.startswith(f"{member}/src/") for member in packages):
                measured.add(path)
    require(measured and roots <= sources.keys(), "untracked or unsupported Cargo source root")
    return packages, roots, sources, measured


def validate_configuration(configuration, packages):
    require(isinstance(configuration, dict) and set(configuration) == {"target_cfg", "package_features"},
            "missing/incompatible measured cfg configuration")
    facts, features = configuration["target_cfg"], configuration["package_features"]
    require(isinstance(facts, list) and all(isinstance(v, str) for v in facts) and
            facts == sorted(set(facts)), "invalid target cfg facts")
    for fact in facts:
        match = re.fullmatch(r'([A-Za-z_][A-Za-z_0-9]*)(?:=(".*"))?', fact)
        require(match is not None, f"invalid target cfg fact: {fact}")
        if match[2] is not None:
            require(isinstance(json.loads(match[2]), str), "non-string target cfg value")
    require({"unix", 'target_os="linux"', 'target_arch="x86_64"'} <= set(facts) and
            "test" not in facts and "windows" not in facts, "cfg facts do not match measured Linux target")
    require(isinstance(features, dict) and set(features) == set(packages), "missing package feature facts")
    for member, variants in features.items():
        require(isinstance(variants, list) and variants, f"missing feature variants: {member}")
        require(all(isinstance(v, list) and all(isinstance(f, str) for f in v) and v == sorted(set(v))
                    for v in variants), f"invalid feature variants: {member}")
        require(variants == [list(v) for v in sorted(set(tuple(v) for v in variants))],
                f"duplicate/unsorted feature variants: {member}")


def measured_configuration(directory, packages):
    """Use compiler-artifact features from the existing nextest build, never a new run."""
    variants = {member: set() for member in packages}
    finished = False
    for line in (directory / "build.jsonl").read_text().splitlines():
        message = json.loads(line)
        require(isinstance(message, dict) and isinstance(message.get("reason"), str), "invalid Cargo JSON message")
        if message["reason"] == "build-finished":
            require(message.get("success") is True, "unsuccessful Cargo build facts")
            finished = True
        if message["reason"] != "compiler-artifact" or "custom-build" in message["target"]["kind"]:
            continue
        manifest = Path(message["manifest_path"])
        require(manifest.is_absolute(), "Cargo manifest path must be absolute")
        try:
            member = manifest.parent.relative_to(ROOT).as_posix()
        except ValueError:
            continue  # Third-party dependency.
        if member not in packages:
            continue
        features = message["features"]
        require(isinstance(features, list) and all(isinstance(f, str) for f in features), "invalid Cargo features")
        variants[member].add(tuple(sorted(set(features))))
    require(finished, "missing successful Cargo build-finished message")
    configuration = {
        "target_cfg": sorted(set((directory / "target-cfg.txt").read_text().splitlines())),
        "package_features": {member: [list(v) for v in sorted(values)] for member, values in variants.items()},
    }
    validate_configuration(configuration, packages)
    return configuration


def read_lcov(data, *, llvm_export=False):
    """Read DA records; LLVM's function-based summary differs from unique file lines."""
    files = {}
    path, lines, lf, lh = None, {}, None, None
    for raw in data.splitlines():
        if raw == "end_of_record":
            require(path is not None, "LCOV record without SF")
            require(lf is not None and lh is not None and lh <= lf, f"invalid LCOV LF/LH: {path}")
            if llvm_export:
                # LLVM sums per-function line summaries for LF/LH, but emits DA
                # from the file-level mapping. Overlapping closures/functions can
                # therefore make the summary differ from the unique DA lines.
                require(lines or lf == 0, f"LCOV summary without DA records: {path}")
            else:
                # Our normalized artifacts always use unique file/line totals.
                require(lf == len(lines) and lh == sum(h > 0 for h in lines.values()),
                        f"inconsistent LCOV LF/LH: {path}")
            target = files.setdefault(path, {})
            for line, hits in lines.items():
                target[line] = target.get(line, 0) + hits
            path, lines, lf, lh = None, {}, None, None
            continue
        require(":" in raw, f"malformed LCOV field: {raw[:80]}")
        key, value = raw.split(":", 1)
        if key == "SF":
            require(path is None and value, "duplicate/empty LCOV SF")
            path = value
        elif key == "DA":
            parts = value.split(",")
            require(path is not None and len(parts) in (2, 3), "DA outside LCOV record")
            require(parts[0].isdigit() and parts[1].isdigit(), "invalid LCOV DA")
            line, hits = map(int, parts[:2])
            require(line > 0 and line not in lines, "duplicate/invalid LCOV line")
            lines[line] = hits
        elif key in ("LF", "LH"):
            require(path is not None and value.isdigit(), "invalid LCOV totals")
            if key == "LF":
                require(lf is None, "duplicate LF")
                lf = int(value)
            else:
                require(lh is None, "duplicate LH")
                lh = int(value)
        else:
            require(key in ("TN", "FN", "FNDA", "FNF", "FNH", "BRDA", "BRF", "BRH", "VER"),
                    f"unknown LCOV field: {key}")
            require(key == "TN" or (path is not None and value), f"LCOV field outside record: {key}")
    require(path is None and bool(files), "empty or truncated LCOV report")
    return files


def write_lcov(path, files):
    with path.open("w") as out:
        for source, lines in sorted(files.items()):
            out.write(f"TN:\nSF:{source}\n")
            for line, hits in sorted(lines.items()):
                out.write(f"DA:{line},{hits}\n")
            out.write(f"LF:{len(lines)}\nLH:{sum(h > 0 for h in lines.values())}\nend_of_record\n")


def relative_sources(files, sources):
    result = {}
    for raw, lines in files.items():
        path = Path(raw)
        if path.is_absolute():
            try:
                path = path.relative_to(ROOT)
            except ValueError:
                continue  # rustc, registry, git dependencies and other external code
        require(".." not in path.parts, f"noncanonical LCOV path: {raw}")
        name = path.as_posix()
        if name not in sources:
            continue  # build scripts, generated target output, tests/ and non-workspace sources
        target = result.setdefault(name, {})
        for line, hits in lines.items():
            target[line] = target.get(line, 0) + hits
    require(result, "LCOV contains no tracked workspace src files")
    return result


def junit_cases(data):
    require(b"<!DOCTYPE" not in data and b"<!ENTITY" not in data, "unsupported JUnit declarations")
    root = ET.fromstring(data)
    require(root.tag == "testsuites", "unexpected JUnit root")
    cases = set()
    for suite in root.findall("testsuite"):
        require(suite.get("name") and suite.get("failures") == "0" and suite.get("errors") == "0",
                "JUnit contains unsuccessful or malformed suite")
        children = suite.findall("testcase")
        require(int(suite.attrib["tests"]) == len(children), "JUnit suite count mismatch")
        for case in children:
            require(case.get("name") and not list(case.findall("skipped")) and
                    not any(n.tag in ("failure", "error", "flakyFailure", "flakyError", "rerunFailure", "rerunError")
                            for n in case), "JUnit contains unsuccessful/retried/skipped testcase")
            key = (suite.attrib["name"], case.get("classname", ""), case.attrib["name"])
            require(key not in cases, f"duplicate JUnit testcase: {key}")
            cases.add(key)
    require(cases and int(root.attrib["tests"]) == len(cases), "missing/inconsistent JUnit test count")
    require(root.get("failures") == "0" and root.get("errors") == "0", "JUnit run was unsuccessful")
    return cases


def ci_identity():
    checkout = command("git", "rev-parse", "HEAD")
    require(checkout == os.environ["GITHUB_SHA"], "checkout differs from GITHUB_SHA")
    event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
    pr = event.get("pull_request")
    return {
        "checkout_sha": checkout,
        "pr_head_sha": pr["head"]["sha"] if pr else None,
        "pr_base_sha": pr["base"]["sha"] if pr else None,
        "event": os.environ["GITHUB_EVENT_NAME"],
        "repository": os.environ["GITHUB_REPOSITORY"],
        "run_id": os.environ["GITHUB_RUN_ID"],
        "run_attempt": os.environ["GITHUB_RUN_ATTEMPT"],
    }


def build_environment():
    keys = ("CARGO_BUILD_JOBS", "CARGO_PROFILE_DEV_DEBUG", "CARGO_PROFILE_TEST_DEBUG",
            "BINDGEN_EXTRA_CLANG_ARGS", "RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "RUSTDOCFLAGS",
            "CARGO_ENCODED_RUSTDOCFLAGS", "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER")
    result = {key: os.environ.get(key) for key in keys}
    require(result["CARGO_BUILD_JOBS"] == "2" and
            result["CARGO_PROFILE_DEV_DEBUG"] == "0" and
            result["CARGO_PROFILE_TEST_DEBUG"] == "0", "unexpected CI build profile")
    require(all(result[k] is None for k in keys[4:]), "unexpected external Rust instrumentation flags")
    result["RUST_MIN_STACK"] = tomllib.loads((ROOT / ".cargo/config.toml").read_text())["env"]["RUST_MIN_STACK"]
    result["CARGO_LLVM_COV_TARGET_DIR"] = "target/coverage"
    return result


def tools():
    result = {
        "rustc": command("rustc", "-vV"),
        "cargo": command("cargo", "--version"),
        "nextest": command("cargo-nextest", "--version"),
        "cargo_llvm_cov": command("cargo-llvm-cov", "--version"),
        "python": platform.python_version(),
        "tree_sitter": importlib.metadata.version("tree-sitter"),
        "tree_sitter_rust": importlib.metadata.version("tree-sitter-rust"),
    }
    validate_tool_versions(result)
    return result


def validate_tool_versions(result):
    require(f"release: {POLICY['rust']}\n" in result["rustc"] + "\n", "unexpected Rust release")
    require(f"host: {POLICY['target']}\n" in result["rustc"] + "\n", "unexpected host target")
    for name, expected in (("nextest", POLICY["nextest"]), ("cargo_llvm_cov", POLICY["cargo_llvm_cov"])):
        require(re.search(rf"(?<![\d.]){re.escape(expected)}(?![\d.])", result[name]), f"unexpected {name} version")


def checks(shard):
    actual = {key: os.environ[f"COVERAGE_{key.upper()}_RESULT"] for key in ("nextest", "fmt", "doctests")}
    expected = {"nextest": "success", "fmt": "success" if shard == 1 else "skipped",
                "doctests": "success" if shard == 1 else "skipped"}
    require(actual == expected, f"incomplete required checks on shard {shard}: {actual}")
    return actual


def pack(shard):
    require(1 <= shard <= POLICY["shards"], "invalid shard")
    packages, roots, sources, measured = inventory()
    out = ROOT / "coverage-shard"
    configuration = measured_configuration(out, packages)
    files = relative_sources(read_lcov((out / "raw.lcov").read_text(), llvm_export=True), measured)
    junit = JUNIT.read_bytes()
    executed = junit_cases(junit)
    write_lcov(out / "shard.lcov", files)
    (out / "junit.xml").write_bytes(junit)
    manifest_hashes = {path: sha((ROOT / path).read_bytes()) for path in
                       ["Cargo.toml", "Cargo.lock", ".cargo/config.toml", "rust-toolchain.toml"] +
                       [f"{member}/Cargo.toml" for member in packages]}
    common = {
        "schema_version": POLICY["schema_version"], "policy_version": POLICY["policy_version"],
        "policy_fingerprint": fingerprint(), "metric": POLICY["metric"], "ci": ci_identity(),
        "platform": {"os": "linux", "arch": "x86_64", "target": POLICY["target"]},
        "tools": tools(), "measurement": POLICY["measurement"], "build_environment": build_environment(),
        "manifest_hashes": manifest_hashes, "packages": packages,
        "source_roots": sorted(roots), "measurement_paths": sorted(measured), "configuration": configuration,
        "source_inventory_sha256": sha(json.dumps({p: sha(s) for p, s in sources.items()}, sort_keys=True).encode()),
    }
    dump(out / "metadata.json", {"common": common, "shard": shard, "partition": f"hash:{shard}/4",
                                 "checks": checks(shard), "tests_executed": len(executed),
                                 "lcov_sha256": sha((out / "shard.lcov").read_bytes()),
                                 "junit_sha256": sha(junit)})
    for name in ("raw.lcov", "build.jsonl", "target-cfg.txt"):
        (out / name).unlink()  # Export only normalized, repository-relative facts.


def merge_shards(directory, identity, policy_fingerprint):
    merged, metadata, seen = {}, [], set()
    expected = {f"shard-{n}" for n in range(1, 5)}
    require({p.name for p in directory.iterdir()} == expected, "expected exactly four shard artifacts")
    common = None
    for shard in range(1, 5):
        folder = directory / f"shard-{shard}"
        item = json.loads((folder / "metadata.json").read_text())
        candidate = item["common"]
        required = {"schema_version", "policy_version", "policy_fingerprint", "metric", "ci", "platform",
                    "tools", "measurement", "build_environment", "manifest_hashes", "packages", "source_inventory_sha256",
                    "source_roots", "measurement_paths", "configuration"}
        require(isinstance(candidate, dict) and required <= candidate.keys(), "incomplete common metadata")
        require(candidate["platform"] == {"os": "linux", "arch": "x86_64", "target": POLICY["target"]},
                "wrong measurement platform")
        validate_tool_versions(candidate["tools"])
        validate_configuration(candidate["configuration"], candidate["packages"])
        require(item["shard"] == shard and item["partition"] == f"hash:{shard}/4", "wrong shard identity")
        require(candidate["ci"] == identity, "different SHA, event, run or attempt")
        require(candidate["policy_fingerprint"] == policy_fingerprint and
                candidate["schema_version"] == POLICY["schema_version"] and
                candidate["policy_version"] == POLICY["policy_version"] and
                candidate["measurement"] == POLICY["measurement"] and candidate["metric"] == POLICY["metric"],
                "incompatible coverage policy or measurement")
        require(common is None or candidate == common, "incompatible shard metadata")
        common = candidate
        require(item["checks"] == {"nextest": "success", "fmt": "success" if shard == 1 else "skipped",
                                    "doctests": "success" if shard == 1 else "skipped"}, "incomplete Linux checks")
        lcov, junit = (folder / "shard.lcov").read_bytes(), (folder / "junit.xml").read_bytes()
        require(sha(lcov) == item["lcov_sha256"] and sha(junit) == item["junit_sha256"], "corrupt shard artifact")
        cases = junit_cases(junit)
        require(len(cases) == item["tests_executed"] and not (seen & cases), "duplicate/inconsistent test execution")
        seen |= cases
        for path, lines in read_lcov(lcov.decode()).items():
            require(not PurePosixPath(path).is_absolute() and ".." not in PurePosixPath(path).parts,
                    "shard LCOV paths must be repository-relative")
            target = merged.setdefault(path, {})
            for line, hits in lines.items():
                target[line] = target.get(line, 0) + hits
        metadata.append(item)
    return merged, common, metadata, len(seen)


def totals(files):
    measured = sum(len(lines) for lines in files.values())
    covered = sum(h > 0 for lines in files.values() for h in lines.values())
    return {"covered_lines": covered, "measurable_lines": measured,
            "percent": covered * 100 / measured if measured else None}


def render_html(out, files, sources, summary):
    directory = out / "html"
    directory.mkdir()
    index = [f"<h1>{html.escape(POLICY['metric'])}</h1>",
             f"<p>SHA: {summary['checkout_sha']}. Complete: 4/4 shards.</p>",
             f"<p>{summary['overall']['covered_lines']}/{summary['overall']['measurable_lines']} lines.</p>"]
    for number, (path, lines) in enumerate(sorted(files.items()), 1):
        name = f"source-{number}.html"
        index.append(f'<p><a href="{name}">{html.escape(path)}</a></p>')
        rows = []
        for line, source in enumerate(sources[path].decode().splitlines(), 1):
            hits = lines.get(line)
            color = "#cfc" if hits and hits > 0 else "#fcc" if hits == 0 else "#eee"
            rows.append(f'<span style="background:{color}">{line:6} {str(hits) if hits is not None else "-":>8} {html.escape(source)}</span>')
        (directory / name).write_text('<!doctype html><meta charset="utf-8"><title>' + html.escape(path) +
                                      '</title><h1>' + html.escape(path) + '</h1><pre>' + '\n'.join(rows) + '</pre>')
    (directory / "index.html").write_text('<!doctype html><meta charset="utf-8"><title>Production coverage</title>' + ''.join(index))


def merge(directory):
    require(os.environ["COVERAGE_LINUX_RESULT"] == "success", "Linux matrix did not succeed")
    identity = ci_identity()
    files, common, shards, count = merge_shards(directory, identity, fingerprint())
    packages, roots, sources, measured = inventory()
    require(common["source_roots"] == sorted(roots) and common["measurement_paths"] == sorted(measured),
            "source roots or measurement scope mismatch")
    require(common["packages"] == packages and common["source_inventory_sha256"] ==
            sha(json.dumps({p: sha(s) for p, s in sources.items()}, sort_keys=True).encode()), "source inventory mismatch")
    require(all(sha((ROOT / path).read_bytes()) == digest for path, digest in common["manifest_hashes"].items()),
            "manifest or Cargo configuration mismatch")
    require(set(files) <= measured, "shard LCOV contains paths outside measurement scope")
    parsed = RustSources(sources, roots, POLICY, measured, packages, common["configuration"])
    production = parsed.filter(files)
    overall = totals(production)
    require(overall["measurable_lines"] > 0, "no measurable production lines; no baseline can be published")
    by_crate = {name: totals({p: lines for p, lines in production.items() if p.startswith(member + "/src/")})
                for member, name in sorted(packages.items())}
    summary = {"schema_version": POLICY["schema_version"], "metric": POLICY["metric"],
               "checkout_sha": identity["checkout_sha"], "complete": True,
               "baseline_requires": "successful coverage artifact upload and final Linux check",
               "shards_expected": 4, "shards_received": len(shards), "tests_executed": count,
               "overall": overall, "crates": by_crate}
    out = ROOT / "coverage-report"
    out.mkdir()  # refuse stale output, including output from a previous attempt
    write_lcov(out / "production.lcov", production)
    dump(out / "summary.json", summary)
    dump(out / "metadata.json", {"common": common, "shards": shards, "complete": True,
                                  "linux_matrix_result": "success"})
    dump(out / "exclusions.json", {"excluded_files": sorted(parsed.excluded_files),
                                    "inline_test_lines": {p: sorted(lines) for p, lines in parsed.ranges.items() if lines}})
    dump(out / "source-hashes.json", {p: sha(source) for p, source in sorted(sources.items())})
    shutil.copy(HERE / "policy.json", out / "policy.json")
    shutil.copytree(directory, out / "shards")
    render_html(out, production, sources, summary)
    md = [f"## {POLICY['metric']}", "", f"Measured checkout SHA: `{identity['checkout_sha']}`.", "",
          f"Complete: **4/4 shards**; **{count} tests executed** (doctests are separate).", "",
          "| Crate | Covered / measurable lines | Coverage |", "| --- | ---: | ---: |"]
    for name, value in [("All workspace crates", overall)] + list(by_crate.items()):
        percent = f"{value['percent']:.2f}%" if value["percent"] is not None else "N/A (no measurable lines)"
        md.append(f"| {name} | {value['covered_lines']} / {value['measurable_lines']} | {percent} |")
    artifact = f"linux-production-coverage-{identity['run_id']}-{identity['run_attempt']}"
    md += ["", f"Download **{artifact}** from this run's Artifacts section; open `html/index.html`.",
           "The artifact includes production LCOV, summary.json, source hashes, exclusion policy and shard metadata.",
           "Retention: 90 days. A baseline requires the coverage upload and final Linux check to succeed.", ""]
    (out / "summary.md").write_text("\n".join(md))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="mode", required=True)
    sub.add_parser("pack").add_argument("--shard", type=int, required=True)
    sub.add_parser("merge").add_argument("--input", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.mode == "pack":
            pack(args.shard)
        else:
            merge(args.input)
    except (CoverageError, KeyError, OSError, ValueError, ET.ParseError) as error:
        parser.exit(1, f"Coverage is incomplete; no baseline published: {error}\n")


if __name__ == "__main__":
    main()
