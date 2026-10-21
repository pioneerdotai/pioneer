"""Report-only production ranges. Rust boundaries come from tree-sitter, not text scans."""

import itertools
import json
import re
from collections import defaultdict, deque
from pathlib import PurePosixPath

from tree_sitter import Language, Parser
import tree_sitter_rust


class CoverageError(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise CoverageError(message)


def text(node):
    return node.text.decode("utf-8")


def string_value(node):
    if node.type == "string_literal":
        # Rust-only escapes are deliberately unsupported in module paths/cfg values.
        return json.loads(text(node))
    if node.type == "raw_string_literal":
        value = text(node)
        return value[value.index('"') + 1:value.rindex('"')]
    raise CoverageError(f"nonliteral path/cfg value: {text(node)}")


def metas(nodes, mode="attributes"):
    """Interpret cfg predicates and relevant attributes; other arguments stay opaque."""
    nodes = [n for n in nodes if n.type not in ("line_comment", "block_comment")]
    result = []
    i = 0
    while i < len(nodes):
        predicate = mode == "predicates" or (mode == "cfg_attr" and not result)
        name = text(nodes[i])
        require(nodes[i].type in ("identifier", "scoped_identifier", "crate"),
                f"unsupported attribute predicate: {name}")
        i += 1
        while i < len(nodes) and text(nodes[i]) == "::":
            require(i + 1 < len(nodes), "incomplete attribute path")
            name += "::" + text(nodes[i + 1])
            i += 2
        value = None
        if i < len(nodes) and text(nodes[i]) == "=":
            i += 1
            require(i < len(nodes), "missing attribute value")
            if predicate or name == "path":
                value = string_value(nodes[i])
                i += 1
            else:
                # e.g. doc = include_str!(...), repr/derive/schemars values.
                while i < len(nodes) and text(nodes[i]) != ",":
                    i += 1
        elif i < len(nodes) and nodes[i].type == "token_tree":
            group = nodes[i]
            if predicate or name in ("cfg", "cfg_attr"):
                require(text(group).startswith("("), "unsupported attribute group")
                value = metas(group.children[1:-1], "cfg_attr" if name == "cfg_attr" else "predicates")
            i += 1
        result.append((name, value))
        if i < len(nodes):
            require(text(nodes[i]) == ",", "unsupported attribute separator")
            i += 1
    return result


def attribute(node):
    attr = node.named_children[0]
    name = text(attr.named_children[0])
    args = attr.child_by_field_name("arguments")
    value = attr.child_by_field_name("value")
    if args is not None:
        # Only interpret predicates that can affect source ownership/exclusion.
        if name in ("cfg", "cfg_attr"):
            return name, metas(args.children[1:-1], "cfg_attr" if name == "cfg_attr" else "predicates")
        return name, None
    return name, string_value(value) if name == "path" and value is not None else None


def production_cfg(meta, policy, configuration=None):
    name, value = meta
    if name == "test" or (name == "feature" and value == policy["test_support_feature"]):
        return {False}
    if name in ("all", "any", "not"):
        require(isinstance(value, list), f"invalid cfg({name})")
        options = [production_cfg(item, policy, configuration) for item in value]
        if name == "not":
            require(len(options) == 1, "invalid cfg(not)")
            return {not v for v in options[0]}
        op = all if name == "all" else any
        return {op(values) for values in itertools.product(*options)}
    if configuration is not None:
        if name == "feature":
            return {value in configuration["features"]}
        if name in ("unix", "windows") or name.startswith("target_"):
            fact = name if value is None else f"{name}={json.dumps(value)}"
            return {fact in configuration["target_cfg"]}
    # Custom build-script cfgs are deliberately not guessed.
    return {False, True}


def mentions_test(meta, policy):
    name, value = meta
    return (name == "test" or
            (name == "feature" and value == policy["test_support_feature"]) or
            (isinstance(value, list) and any(mentions_test(m, policy) for m in value)))


def classify(attrs, policy, configuration=None):
    """Return (test-only, explicit module path). Reject unresolved test-sensitive cfgs."""
    excluded, path = False, None
    for name, value in attrs:
        if name.split("::")[-1] in policy["test_attributes"]:
            excluded = True
        elif name == "path":
            require(isinstance(value, str) and path is None, "ambiguous module path")
            path = value
        elif name == "cfg":
            require(isinstance(value, list) and len(value) == 1, "invalid cfg attribute")
            possible = production_cfg(value[0], policy, configuration)
            if mentions_test(value[0], policy):
                require(len(possible) == 1, f"ambiguous production cfg: {value}")
            excluded |= possible == {False}
        elif name == "cfg_attr":
            require(isinstance(value, list) and len(value) >= 2, "invalid cfg_attr")
            condition = production_cfg(value[0], policy, configuration)
            if condition == {False}:
                continue
            nested_excluded, nested_path = classify(value[1:], policy, configuration)
            if nested_excluded or nested_path is not None:
                require(len(condition) == 1, f"ambiguous cfg_attr: {value}")
                if condition == {True}:
                    excluded |= nested_excluded
                    require(path is None or nested_path is None, "ambiguous cfg_attr path")
                    path = path if nested_path is None else nested_path
    return excluded, path


def macro_has_tests(attrs, policy, configuration):
    """Inspect test-generating metadata, not disabled production cfg branches."""
    for name, value in attrs:
        if name.split("::")[-1] in policy["test_attributes"]:
            return True
        if name == "cfg":
            require(isinstance(value, list) and len(value) == 1, "invalid macro cfg")
            if mentions_test(value[0], policy) and classify([(name, value)], policy, configuration)[0]:
                return True
        elif name == "cfg_attr":
            require(isinstance(value, list) and len(value) >= 2, "invalid macro cfg_attr")
            # Macro attributes expand at the call site. A producer-side false
            # predicate must not hide a nested test attribute from this guard.
            if macro_has_tests(value[1:], policy, configuration):
                return True
    return False


def generated_include(group):
    """Only the project's concat!(env!("OUT_DIR"), "/literal.rs") form is known."""
    def tokens(node):
        if node.type == "token_tree":
            for child in node.children:
                yield from tokens(child)
        elif node.type not in ("line_comment", "block_comment"):
            yield node
    nodes = list(tokens(group))
    if len(nodes) != 13:
        return False
    if ([text(n) for n in nodes[:7]] != ["(", "concat", "!", "(", "env", "!", "("] or
            [text(n) for n in nodes[8:10]] != [")", ","] or
            [text(n) for n in nodes[11:]] != [")", ")"]):
        return False
    directory, suffix = string_value(nodes[7]), string_value(nodes[10])
    return (directory == "OUT_DIR" and suffix.startswith("/") and suffix.endswith(".rs") and
            ".." not in PurePosixPath(suffix).parts)


class _RustSourceVariant:
    def __init__(self, sources, roots, policy, measured=None, configuration=None):
        """sources maps repository-relative paths to bytes; roots are Cargo source roots."""
        self.sources, self.policy = sources, policy
        self.measured = set(sources) if measured is None else set(measured)
        self.configuration = configuration
        self.ranges = defaultdict(set)
        self.edges = defaultdict(list)
        self.excluded_files = set()
        test_paths = set()
        parser = Parser(Language(tree_sitter_rust.language()))
        pattern = re.compile(policy["excluded_filename_regex"])
        # File policy is independent of the module's inclusion context.
        for path, source in sources.items():
            parts = PurePosixPath(path).parts
            header = source.lstrip().split(b"\n", 1)[0]
            if (any(p in policy["excluded_components"] for p in parts) or
                    pattern.search(parts[-1]) or
                    (header.startswith(b"//") and
                     policy["generated_header_marker"].encode() in header)):
                self.excluded_files.add(path)
                # Generated code is excluded by file, but may refer to authored modules.
                if pattern.search(parts[-1]) or any(p in ("tests", "fixtures", "testdata", "benches", "examples") for p in parts):
                    test_paths.add(path)
        # A physical file can be loaded by normal mod, #[path] and include! with
        # different child directories. Keep their edges and test scopes separate.
        root_contexts = [(p, PurePosixPath(p).parent, False) for p in sorted(roots)]
        remaining = set(self.measured) | set(roots)
        pending_sources = deque(root_contexts)
        scanned, trees = set(), {}
        while pending_sources or remaining:
            if pending_sources:
                path, directory, excluded = pending_sources.popleft()
            else:
                # Still classify measured files absent from the Cargo root walk.
                path = min(remaining)
                file = PurePosixPath(path)
                directory = file.parent if file.name == "mod.rs" else file.with_suffix("")
                excluded = False
            context = (path, directory, excluded or path in test_paths)
            if context in scanned:
                continue
            scanned.add(context)
            remaining.discard(path)
            if path not in trees:
                trees[path] = parser.parse(sources[path])
                require(not trees[path].root_node.has_error, f"Rust syntax could not be classified: {path}")
            self._scan(path, trees[path].root_node, directory, PurePosixPath(path).parent, context[2], context)
            pending_sources.extend(self.edges[context])
        contexts = set()
        pending = deque(root_contexts)
        while pending:
            path, directory, excluded = pending.popleft()
            context = (path, directory, excluded or path in test_paths)
            if context in contexts:
                continue
            contexts.add(context)
            pending.extend(self.edges[context])
        self.production_files = {p for p, _, excluded in contexts if not excluded}
        self.test_files = {p for p, _, excluded in contexts if excluded} - self.production_files
        self.excluded_files |= self.test_files

    def _exclude(self, path, node, first=None):
        first = node if first is None else first
        source = self.sources[path]
        start, end = first.start_byte, node.end_byte
        row_start = source.rfind(b"\n", 0, start) + 1
        row_end = source.find(b"\n", end)
        row_end = len(source) if row_end < 0 else row_end
        # A line metric cannot separate test and production statements on one line.
        suffix = source[end:row_end].lstrip(b",; \t").strip()
        require(not source[row_start:start].strip() and (not suffix or suffix.startswith(b"//")),
                f"mixed test/production boundary on one line: {path}:{first.start_point.row + 1}")
        last_row = node.end_point.row + (node.end_point.column != 0)
        self.ranges[path].update(range(first.start_point.row + 1, last_row + 1))

    def _target(self, path, directory, name, explicit):
        if explicit is not None:
            candidates = [directory / explicit]
        else:
            candidates = [directory / (name + ".rs"), directory / name / "mod.rs"]
        # Normalize ../ only within the repository, without following filesystem links.
        normalized = []
        for candidate in candidates:
            parts = []
            for part in candidate.parts:
                if part == "..":
                    require(bool(parts), f"module path escapes repository: {path}")
                    parts.pop()
                elif part != ".":
                    parts.append(part)
            normalized.append("/".join(parts))
        found = [p for p in normalized if p in self.sources]
        require(len(found) <= 1, f"ambiguous external module: {path}, {name}")
        return found[0] if found else None

    def _scan(self, path, parent, directory, path_directory, inherited, context):
        # These grammar nodes own their attributes; list-level attributes instead
        # apply to the following sibling item. In particular, cfg on a struct
        # initializer must remove the value as well as the field identifier.
        attached = parent.type in ("field_initializer", "shorthand_field_initializer", "match_arm")
        if attached:
            attributes = [n for n in parent.named_children if n.type == "attribute_item"]
            excluded, explicit = classify([attribute(n) for n in attributes], self.policy, self.configuration)
            require(explicit is None, f"path on an expression: {path}")
            if excluded and not inherited:
                self._exclude(path, parent, attributes[0])
            inherited |= excluded
        inner = [attribute(n) for n in parent.named_children if n.type == "inner_attribute_item"]
        inner_excluded, inner_path = classify(inner, self.policy, self.configuration)
        require(inner_path is None, f"unsupported inner path attribute: {path}")
        if inner_excluded and not inherited:
            scope = parent.parent if parent.type == "declaration_list" else parent
            self._exclude(path, scope)
        inherited |= inner_excluded
        attrs, first = [], None
        for node in parent.named_children:
            if node.type in ("line_comment", "block_comment", "inner_attribute_item"):
                continue
            if node.type == "attribute_item":
                if attached:
                    continue
                attrs.append(attribute(node))
                first = node if first is None else first
                continue
            excluded, explicit = classify(attrs, self.policy, self.configuration)
            local = inherited or excluded
            if excluded and not inherited:
                self._exclude(path, node, first)
            attrs, first = [], None
            if node.type == "mod_item":
                name = text(node.child_by_field_name("name"))
                body = node.child_by_field_name("body")
                if body is None:
                    target = self._target(path, path_directory if explicit else directory, name, explicit)
                    if target:
                        file = PurePosixPath(target)
                        target_directory = file.parent if explicit is not None or file.name == "mod.rs" else file.with_suffix("")
                        self.edges[context].append((target, target_directory, local))
                else:
                    require(explicit is None, f"path on inline module is unsupported: {path}")
                    self._scan(path, body, directory / name, directory / name, local, context)
            elif node.type in ("macro_invocation", "macro_definition"):
                macro = node.child_by_field_name("macro")
                if macro is not None and text(macro) == "include":
                    group = next(n for n in node.named_children if n.type == "token_tree")
                    literals = [n for n in group.named_children if n.type in ("string_literal", "raw_string_literal")]
                    if len(group.named_children) == 1 and len(literals) == 1:
                        target = self._target(path, PurePosixPath(path).parent, "include", string_value(literals[0]))
                        if target:
                            # include! loads items relative to the included file's directory.
                            self.edges[context].append((target, PurePosixPath(target).parent, local))
                    else:
                        require(generated_include(group), f"nonliteral include! cannot be classified: {path}")
                if not local:
                    self._check_macro(path, node)
            else:
                require(explicit is None, f"path on non-module item: {path}")
                self._scan(path, node, directory, path_directory, local, context)

    def _check_macro(self, path, node):
        # Never interpret a macro's token tree as ordinary Rust. If it can introduce
        # test-only items, require an explicit enclosing test scope instead.
        for i, child in enumerate(node.children):
            if text(child) == "#" and i + 1 < len(node.children):
                group = node.children[i + 1]
                if group.type == "token_tree" and text(group).startswith("["):
                    names = [text(n) for n in group.named_children if n.type in ("identifier", "scoped_identifier")]
                    if names and (names[0] in ("cfg", "cfg_attr") or
                                  any(name.split("::")[-1] in self.policy["test_attributes"] for name in names)):
                        content = metas(group.children[1:-1])
                        require(not macro_has_tests(content, self.policy, self.configuration),
                                f"test-generating macro outside test scope: {path}")
            if child.type not in ("string_literal", "raw_string_literal", "line_comment", "block_comment"):
                self._check_macro(path, child)

    def filter(self, files):
        result = {}
        for path, lines in files.items():
            require(path in self.sources, f"coverage references unknown source: {path}")
            if path not in self.measured or path in self.excluded_files:
                continue
            require(path in self.production_files, f"unresolved source ownership: {path}")
            count = len(self.sources[path].splitlines())
            require(all(0 < line <= count for line in lines), f"coverage line outside source: {path}")
            result[path] = {line: hits for line, hits in lines.items() if line not in self.ranges[path]}
        return {path: lines for path, lines in result.items() if lines}


class RustSources(_RustSourceVariant):
    def __init__(self, sources, roots, policy, measured=None, packages=None, configuration=None):
        if packages is None:
            super().__init__(sources, roots, policy, measured, configuration)
            return
        self.sources = sources
        self.measured = set(sources) if measured is None else set(measured)
        self.ranges = defaultdict(set)
        self.excluded_files, self.production_files = set(), set()
        require(configuration is not None, "missing measured cfg configuration")
        for member in packages:
            owned = {p: source for p, source in sources.items() if p.startswith(member + "/")}
            member_roots = roots & owned.keys()
            variants = [_RustSourceVariant(owned, member_roots, policy, self.measured & owned.keys(),
                        {"target_cfg": configuration["target_cfg"], "features": features})
                        for features in configuration["package_features"][member]]
            for path in owned:
                production = [v for v in variants if path in v.production_files and path not in v.excluded_files]
                if production:
                    self.production_files.add(path)
                    # Keep a line if it is production in ANY actual compilation variant.
                    self.ranges[path] = set.intersection(*(v.ranges[path] for v in production))
                elif any(path in v.excluded_files for v in variants):
                    # Test-only in a variant and absent from the others is still test-only.
                    # Files never owned/excluded by any variant remain unresolved.
                    self.excluded_files.add(path)
