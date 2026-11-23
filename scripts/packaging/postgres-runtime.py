#!/usr/bin/env python3
"""Build a relocatable PostgreSQL and pgvector release input from pinned sources."""

import argparse
import hashlib
import os
from pathlib import Path
import runpy
import shutil
import subprocess
import tarfile
import tempfile
import urllib.request

PG_VERSION = "19.0"
VECTOR_VERSION = "0.8.7"
SOURCES = {
    f"postgresql-{PG_VERSION}": (
        f"https://ftp.postgresql.org/pub/source/v{PG_VERSION}/postgresql-{PG_VERSION}.tar.bz2",
        "a89fd70efbad6a08c7dbed8d4872e3af30170d9e5ca9dc7906523be95b4e692d",
    ),
    f"pgvector-{VECTOR_VERSION}": (
        f"https://codeload.github.com/pgvector/pgvector/tar.gz/refs/tags/v{VECTOR_VERSION}",
        "cac0b10c360f05b2d521200105ba3697e773d4cd3731f5a915a7e37ebe0bea85",
    ),
}
PROGRAMS = ("postgres", "initdb", "pg_ctl", "psql")


def postgres_binary_target(target):
    # Windows 11 ARM64 runs the bundled x64 server as a separate emulated process.
    if target == "aarch64-pc-windows-msvc":
        return "x86_64-pc-windows-msvc"
    return target


def source(work, name):
    url, expected = SOURCES[name]
    archive = work / f"{name}.tar"
    with urllib.request.urlopen(url, timeout=60) as response, archive.open("wb") as output:
        shutil.copyfileobj(response, output)
    with archive.open("rb") as handle:
        if hashlib.file_digest(handle, "sha256").hexdigest() != expected:
            raise ValueError(f"Source checksum mismatch for {name}")
    with tarfile.open(archive) as tar:
        tar.extractall(work, filter="data")
    archive.unlink()
    return work / name


def run(*args, cwd, env):
    subprocess.run(args, cwd=cwd, env=env, check=True)


def build_unix(pg, vector, prefix, env, jobs):
    run(
        str(pg / "configure"), f"--prefix={prefix}", "--disable-rpath",
        "--without-icu", "--without-readline", "--without-zlib",
        cwd=pg, env=env,
    )
    run("make", f"-j{jobs}", "world-bin", cwd=pg, env=env)
    run("make", "install-world-bin", cwd=pg, env=env)
    run(
        "make", f"-j{jobs}", f"PG_CONFIG={prefix / 'bin/pg_config'}", "OPTFLAGS=",
        cwd=vector, env=env,
    )
    run(
        "make", f"PG_CONFIG={prefix / 'bin/pg_config'}", "OPTFLAGS=", "install",
        cwd=vector, env=env,
    )


def build_windows(pg, vector, prefix, env, jobs):
    # The release runner loads the native MSVC environment before invoking
    # this script. Meson builds both PostgreSQL and its extension import lib.
    build = pg.parent / "pg-build"
    run(
        "meson", "setup", str(build), str(pg), f"--prefix={prefix}", "--libdir=lib",
        "--buildtype=release", "-Db_vscrt=mt", "-Dauto_features=disabled", "-Dssl=none",
        cwd=pg, env=env,
    )
    run("meson", "compile", "-C", str(build), "-j", str(jobs), cwd=pg, env=env)
    run("meson", "install", "-C", str(build), cwd=pg, env=env)
    if not (prefix / "lib/postgres.lib").is_file():
        shutil.copy2(build / "src/backend/postgres.lib", prefix / "lib/postgres.lib")
    extension_env = {**env, "_CL_": "/MT"}
    run("nmake", "/f", "Makefile.win", f"PGROOT={prefix}", "install", cwd=vector, env=extension_env)


def relocate_macos(prefix, destination):
    for item in sorted([*destination.joinpath("bin").iterdir(), *destination.joinpath("lib").iterdir()]):
        if not item.is_file():
            continue
        if item.suffix == ".dylib":
            subprocess.run(["install_name_tool", "-id", f"@rpath/{item.name}", item], check=True)
        dependencies = subprocess.check_output(["otool", "-L", item], text=True)
        for line in dependencies.splitlines()[1:]:
            path = line.strip().split(" (", 1)[0]
            if path.startswith(str(prefix / "lib") + "/"):
                library = destination / "lib" / Path(path).name
                relative = os.path.relpath(library, item.parent)
                subprocess.run(
                    ["install_name_tool", "-change", path, f"@loader_path/{relative}", item],
                    check=True,
                )
        subprocess.run(["codesign", "--force", "--sign", "-", item], check=True)


def copy_runtime(pg, vector, prefix, destination, target):
    windows = "windows" in target
    suffix = ".exe" if windows else ""
    (destination / "bin").mkdir(parents=True)
    (destination / "lib").mkdir()
    for program in PROGRAMS:
        name = program + suffix
        shutil.copyfile(prefix / "bin" / name, destination / "bin" / name)
    if windows:
        for path in (prefix / "bin").glob("*.dll"):
            shutil.copyfile(path, destination / "bin" / path.name)
    # Expand shared-library name aliases into regular files for portable
    # archive extraction. The runtime excludes development headers and PGXS.
    for path in (prefix / "lib").iterdir():
        if path.is_file() and (path.suffix in (".dll", ".dylib") or ".so" in path.name):
            shutil.copyfile(path, destination / "lib" / path.name)
    shutil.copytree(prefix / "share", destination / "share")
    shutil.copyfile(pg / "COPYRIGHT", destination / "LICENSE.postgresql")
    shutil.copyfile(vector / "LICENSE", destination / "LICENSE.pgvector")
    if "apple-darwin" in target:
        relocate_macos(prefix, destination)
    for path in (destination / "bin").iterdir():
        path.chmod(0o755)


def validate(target, root):
    suffix = ".exe" if "windows" in target else ""
    module = "vector.dll" if suffix else "vector.so"
    if "apple-darwin" in target and (root / "lib/vector.dylib").is_file():
        module = "vector.dylib"
    architecture = runpy.run_path(str(Path(__file__).with_name("cua-driver.py")))["architecture"]
    required = [
        root / "lib" / module,
        root / "share/extension/vector.control",
        root / f"share/extension/vector--{VECTOR_VERSION}.sql",
        root / "share/postgres.bki",
        root / "LICENSE.postgresql",
        root / "LICENSE.pgvector",
    ]
    for path in required:
        if not path.is_file():
            raise ValueError(f"Missing bundled PostgreSQL input: {path.relative_to(root)}")
    for path in [root / "lib" / module, *(root / "bin" / (name + suffix) for name in PROGRAMS)]:
        architecture(path, postgres_binary_target(target))


def prepare(target, destination, jobs):
    destination = destination.resolve()
    if destination.exists():
        raise ValueError("PostgreSQL runtime output already exists")
    env = os.environ.copy()
    if "apple-darwin" in target:
        env["MACOSX_DEPLOYMENT_TARGET"] = "13.0"
        # Permit relocatable install names in the staged runtime.
        env["LDFLAGS"] = "-Wl,-headerpad_max_install_names"
    with tempfile.TemporaryDirectory(prefix="pioneer-pg-build-") as tmp:
        work = Path(tmp)
        pg = source(work, f"postgresql-{PG_VERSION}")
        vector = source(work, f"pgvector-{VECTOR_VERSION}")
        prefix = work / "postgres-install"
        if "windows" in target:
            build_windows(pg, vector, prefix, env, jobs)
        else:
            build_unix(pg, vector, prefix, env, jobs)
        staged = work / "runtime"
        copy_runtime(pg, vector, prefix, staged, target)
        validate(target, staged)
        if "linux" in target:
            env["LD_LIBRARY_PATH"] = str(staged / "lib")
        suffix = ".exe" if "windows" in target else ""
        for name in PROGRAMS:
            binary = staged / "bin" / (name + suffix)
            version = subprocess.check_output([binary, "--version"], env=env, text=True)
            if version.split()[-1] != PG_VERSION:
                raise ValueError(f"Unexpected {name} version")
        shutil.copytree(staged, destination)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--jobs", type=int, default=4)
    args = parser.parse_args()
    prepare(args.target, args.out, args.jobs)
