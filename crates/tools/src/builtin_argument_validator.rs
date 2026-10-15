//! Schema diagnostics for builtins only. Never format a validator error: its
//! Display implementation may include argument values or schema enum values.
use crate::ToolError;
use jsonschema::{
    ValidationError, Validator,
    error::{TypeKind, ValidationErrorKind},
};
use serde::Serialize;
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

const MAX_DIAGNOSTICS: usize = 32;
const MAX_DIAGNOSTIC_BYTES: usize = 8 * 1024;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Diagnostic {
    path: String,
    code: &'static str,
    expected: String,
    actual_type: &'static str,
}

pub(crate) fn compile(schema: &Value) -> Result<Validator, ()> {
    // External retrieval is disabled in Cargo features. Builtin schemas use
    // local references; an invalid/unresolvable schema fails closed.
    jsonschema::options().build(schema).map_err(|_| ())
}

pub(crate) fn validate(
    arguments: &Value,
    schema: &Value,
    validator: &Validator,
) -> Result<(), ToolError> {
    let mut names = BTreeSet::new();
    schema_property_names(schema, &mut names);
    let mut diagnostics = Vec::new();
    let mut hidden = 0usize;
    let mut bytes = 0usize;
    let mut complete = true;
    let mut composite_details_truncated = false;
    let mut branch_validators = BTreeMap::new();
    let mut record = |diagnostic: Diagnostic| {
        let size = serde_json::to_vec(&diagnostic)
            .expect("diagnostic serializes")
            .len();
        if diagnostics.len() < MAX_DIAGNOSTICS && bytes + size <= MAX_DIAGNOSTIC_BYTES {
            bytes += size;
            diagnostics.push(diagnostic);
        } else {
            hidden += 1;
        }
    };
    for error in validator.iter_errors(arguments) {
        record_error(
            error,
            schema,
            "",
            arguments,
            "",
            &names,
            &mut record,
            &mut complete,
            &mut composite_details_truncated,
            &mut branch_validators,
            0,
        );
    }
    if diagnostics.is_empty() && hidden == 0 {
        return Ok(());
    }
    Err(ToolError::invalid_arguments(
        serde_json::json!({
            "code": "builtin_argument_schema_error",
            "diagnostics": diagnostics,
            "validationComplete": complete,
            "diagnosticsTruncated": hidden > 0 || composite_details_truncated,
            "compositeDetailsTruncated": composite_details_truncated,
            "hiddenDiagnosticCount": hidden,
        })
        .to_string(),
    ))
}

// `apply().basic()` exposes rejected alternatives too, but only as rendered
// strings. Keep typed iter_errors diagnostics and use the same validator for
// branch probes. Parent type or a positively matched literal discriminator
// must establish a unique alternative; child content errors do not choose it.
const SCHEMA_RESOURCE: &str = "urn:pioneer:builtin-argument-diagnostics";
const MAX_COMPOSITE_DEPTH: usize = 128;

fn record_error(
    error: ValidationError<'_>,
    schema: &Value,
    schema_base: &str,
    arguments: &Value,
    instance_base: &str,
    names: &BTreeSet<String>,
    record: &mut dyn FnMut(Diagnostic),
    complete: &mut bool,
    composite_details_truncated: &mut bool,
    branch_validators: &mut BTreeMap<String, Arc<Validator>>,
    depth: usize,
) {
    if matches!(
        error.kind(),
        ValidationErrorKind::AnyOf { .. } | ValidationErrorKind::OneOfNotValid { .. }
    ) {
        if depth < MAX_COMPOSITE_DEPTH {
            if let Some((pointer, validator)) =
                selected_alternative(&error, schema, schema_base, branch_validators, complete)
            {
                let base = format!("{instance_base}{}", error.instance_path());
                for child in validator.iter_errors(error.instance()) {
                    // This validator's root is a reference to the selected slice.
                    record_error(
                        child,
                        schema,
                        &pointer,
                        arguments,
                        &base,
                        names,
                        record,
                        complete,
                        composite_details_truncated,
                        branch_validators,
                        depth + 1,
                    );
                }
                return;
            }
        } else {
            *composite_details_truncated = true; // The full validator completed; only detail expansion is bounded.
        }
    }
    let (code, expected) = describe(error.kind());
    if validation_unavailable(error.kind()) {
        *complete = false;
    }
    let pointer = format!("{instance_base}{}", error.instance_path());
    if let Some(properties) =
        grouped_false_schema_properties(&error, schema, schema_base, arguments, &pointer)
    {
        for (name, value) in properties {
            record(Diagnostic {
                path: safe_path(&format!("{pointer}/{}", escape(name)), arguments, &names),
                code: "additionalProperties",
                expected: "only properties allowed by schema".to_owned(),
                actual_type: value_type(value),
            });
        }
        return;
    }
    // These library errors group independent unexpected properties/items.
    // Expand them without exposing model-authored dynamic property names.
    if let ValidationErrorKind::AdditionalProperties { unexpected }
    | ValidationErrorKind::UnevaluatedProperties { unexpected } = error.kind()
    {
        for name in unexpected {
            record(Diagnostic {
                path: safe_path(&format!("{pointer}/{}", escape(name)), arguments, &names),
                code,
                expected: expected.clone(),
                actual_type: error
                    .instance()
                    .get(name)
                    .map(value_type)
                    .unwrap_or("unknown"),
            });
        }
        return;
    }
    if let ValidationErrorKind::AdditionalItems { limit } = error.kind() {
        if let Some(items) = error.instance().as_array() {
            for (index, item) in items.iter().enumerate().skip(*limit) {
                record(Diagnostic {
                    path: safe_path(&format!("{pointer}/{index}"), arguments, &names),
                    code,
                    expected: expected.clone(),
                    actual_type: value_type(item),
                });
            }
            return;
        }
    }
    let mut path = safe_path(&pointer, arguments, &names);
    let actual_type = if let ValidationErrorKind::Required { property } = error.kind() {
        if let Some(name) = property.as_str() {
            path.push('/');
            path.push_str(&escape(name)); // Schema-authored required property.
        }
        "missing"
    } else {
        value_type(error.instance())
    };
    let diagnostic = Diagnostic {
        path,
        code,
        expected,
        actual_type,
    };
    record(diagnostic);
}

fn selected_alternative(
    error: &ValidationError<'_>,
    schema: &Value,
    base: &str,
    branch_validators: &mut BTreeMap<String, Arc<Validator>>,
    complete: &mut bool,
) -> Option<(String, Arc<Validator>)> {
    let pointer = validation_schema_location(schema, base, error)?;
    let alternatives = schema.pointer(&pointer)?.as_array()?;
    let mut candidates = Vec::new();
    for index in 0..alternatives.len() {
        let branch_pointer = format!("{pointer}/{index}");
        let validator = slice_validator(schema, &branch_pointer, branch_validators)?;
        let mut root_type_mismatch = false;
        let mut literal_failures = BTreeSet::new();
        let mut branch_complete = true;
        for failure in validator.iter_errors(error.instance()) {
            branch_complete &= !validation_unavailable(failure.kind());
            match failure.kind() {
                ValidationErrorKind::Type { .. }
                    if failure.instance_path().to_string().is_empty() =>
                {
                    root_type_mismatch = true;
                }
                ValidationErrorKind::Constant { .. } | ValidationErrorKind::Enum { .. } => {
                    literal_failures.insert(failure.instance_path().to_string());
                }
                _ => {}
            }
        }
        if !root_type_mismatch {
            *complete &= branch_complete;
            candidates.push((branch_pointer, validator, literal_failures));
        }
    }
    // An object/null union is selected by the parent type, regardless of
    // invalid enum/const content inside that object.
    if candidates.len() == 1 {
        let (pointer, validator, _) = candidates.pop()?;
        return Some((pointer, validator));
    }
    let mut selected = None;
    for (index, (pointer, validator, failures)) in candidates.iter().enumerate() {
        let mut proven = false;
        for path in candidates.iter().flat_map(|(_, _, failures)| failures) {
            if failures.contains(path) {
                continue;
            }
            if !candidates
                .iter()
                .enumerate()
                .all(|(other, (_, _, failures))| other == index || failures.contains(path))
            {
                continue;
            }
            let Some(value) = error.instance().pointer(path) else {
                continue;
            };
            let tokens = path.split('/').skip(1).collect::<Vec<_>>();
            // Positive evidence is required: this alternative actually declares
            // a const/enum at the same location and that schema accepts the tag.
            for selector in literal_selector_locations(schema, pointer, &tokens, 0) {
                if slice_validator(schema, &selector, branch_validators)?.is_valid(value) {
                    proven = true;
                    break;
                }
            }
            if proven {
                break;
            }
        }
        if proven {
            if selected.is_some() {
                return None;
            } // Conflicting discriminator axes.
            selected = Some((pointer.clone(), validator.clone()));
        }
    }
    selected
}

fn grouped_false_schema_properties<'a>(
    error: &ValidationError<'_>,
    schema: &Value,
    schema_base: &str,
    arguments: &'a Value,
    pointer: &str,
) -> Option<&'a serde_json::Map<String, Value>> {
    if !matches!(error.kind(), ValidationErrorKind::FalseSchema) {
        return None;
    }
    let location = validation_schema_location(schema, schema_base, error)?;
    let (parent, keyword) = location.rsplit_once('/')?;
    let definition = schema.pointer(parent)?;
    if keyword != "additionalProperties"
        || definition.get(keyword) != Some(&Value::Bool(false))
        || definition.get("properties").is_some()
        || definition.get("patternProperties").is_some()
    {
        return None;
    }
    let instance = arguments.pointer(pointer)?;
    // jsonschema's no-properties shortcut reports the first property's value
    // at its containing object's path. A genuine false schema reports the
    // instance at that path; keep that error intact, including keyword-like names.
    if instance == error.instance().as_ref() {
        return None;
    }
    instance.as_object()
}

fn validation_schema_location(
    schema: &Value,
    base: &str,
    error: &ValidationError<'_>,
) -> Option<String> {
    // Diagnostic expansion needs the traversal path, including local $ref
    // steps; schema_path() now reports the canonical location instead.
    let path = error.evaluation_path().to_string();
    let path = if base.is_empty() {
        path.as_str()
    } else {
        path.strip_prefix("/$ref")?
    };
    schema_location(schema, base, path)
}

fn slice_validator(
    schema: &Value,
    pointer: &str,
    cache: &mut BTreeMap<String, Arc<Validator>>,
) -> Option<Arc<Validator>> {
    if let Some(validator) = cache.get(pointer) {
        return Some(validator.clone());
    }
    // Cache size depends on schema locations, not invalid array element count.
    let registry = jsonschema::Registry::new()
        .add(SCHEMA_RESOURCE, schema.clone())
        .ok()?
        .prepare()
        .ok()?;
    let validator = Arc::new(
        jsonschema::options()
            .with_registry(&registry)
            .build(&serde_json::json!({"$ref": format!("{SCHEMA_RESOURCE}#{pointer}")}))
            .ok()?,
    );
    cache.insert(pointer.to_owned(), validator.clone());
    Some(validator)
}

// Locate necessary literal selectors only, through properties, local refs and
// allOf. This is not a validator: the library checks each located schema slice.
// Conditional/alternative selectors without this proof retain aggregate errors.
fn literal_selector_locations(
    root: &Value,
    pointer: &str,
    tokens: &[&str],
    depth: usize,
) -> Vec<String> {
    if depth >= MAX_COMPOSITE_DEPTH {
        return Vec::new();
    }
    let Some(node) = root.pointer(pointer) else {
        return Vec::new();
    };
    if let Some(reference) = node.get("$ref").and_then(Value::as_str) {
        return reference
            .strip_prefix('#')
            .map(|pointer| literal_selector_locations(root, pointer, tokens, depth + 1))
            .unwrap_or_default();
    }
    let mut locations = Vec::new();
    if tokens.is_empty() {
        if node.get("const").is_some() || node.get("enum").is_some() {
            locations.push(pointer.to_owned());
        }
    } else if node
        .get("properties")
        .and_then(|properties| properties.pointer(&format!("/{}", tokens[0])))
        .is_some()
    {
        locations.extend(literal_selector_locations(
            root,
            &format!("{pointer}/properties/{}", tokens[0]),
            &tokens[1..],
            depth + 1,
        ));
    }
    if let Some(all) = node.get("allOf").and_then(Value::as_array) {
        for index in 0..all.len() {
            locations.extend(literal_selector_locations(
                root,
                &format!("{pointer}/allOf/{index}"),
                tokens,
                depth + 1,
            ));
        }
    }
    locations
}

// Library keyword locations retain `$ref` evaluation steps. Resolve those
// steps against the original schema rather than interpreting reference text
// as a JSON property or losing the enclosing definitions in a schema slice.
fn schema_location(root: &Value, base: &str, path: &str) -> Option<String> {
    let mut pointer = base.to_owned();
    let mut node = root.pointer(&pointer)?;
    for token in path.split('/').skip(1) {
        if token == "$ref" {
            pointer = node.get("$ref")?.as_str()?.strip_prefix('#')?.to_owned();
            node = root.pointer(&pointer)?;
        } else {
            pointer.push('/');
            pointer.push_str(token);
            node = node.pointer(&format!("/{token}"))?;
        }
    }
    Some(pointer)
}

fn validation_unavailable(kind: &ValidationErrorKind) -> bool {
    match kind {
        ValidationErrorKind::BacktrackLimitExceeded { .. }
        | ValidationErrorKind::RegexEngineFailure { .. }
        | ValidationErrorKind::Referencing(_)
        | ValidationErrorKind::Custom { .. } => true,
        ValidationErrorKind::PropertyNames { error } => validation_unavailable(error.kind()),
        _ => false,
    }
}

fn describe(kind: &ValidationErrorKind) -> (&'static str, String) {
    use ValidationErrorKind::*;
    let (code, expected) = match kind {
        Type {
            kind: TypeKind::Single(kind),
        } => return ("type", kind.to_string()),
        Type {
            kind: TypeKind::Multiple(kinds),
        } => {
            return (
                "type",
                kinds
                    .into_iter()
                    .map(|kind| kind.to_string())
                    .collect::<Vec<_>>()
                    .join(" or "),
            );
        }
        Required { .. } => ("required", "present required property"),
        AnyOf { .. } => ("anyOf", "match at least one schema alternative"),
        OneOfNotValid { .. } | OneOfMultipleValid { .. } => {
            ("oneOf", "match exactly one schema alternative")
        }
        AdditionalProperties { .. } | UnevaluatedProperties { .. } => {
            ("additionalProperties", "only properties allowed by schema")
        }
        AdditionalItems { .. } | UnevaluatedItems { .. } => {
            ("additionalItems", "only items allowed by schema")
        }
        Enum { .. } => ("enum", "a schema enum option"),
        Constant { .. } => ("const", "the schema constant"),
        MaxItems { limit } => return ("maxItems", format!("at most {limit} items")),
        MinItems { limit } => return ("minItems", format!("at least {limit} items")),
        MaxLength { limit } => return ("maxLength", format!("at most {limit} characters")),
        MinLength { limit } => return ("minLength", format!("at least {limit} characters")),
        MaxProperties { limit } => return ("maxProperties", format!("at most {limit} properties")),
        MinProperties { limit } => {
            return ("minProperties", format!("at least {limit} properties"));
        }
        Maximum { .. } => ("maximum", "number within schema maximum"),
        Minimum { .. } => ("minimum", "number within schema minimum"),
        ExclusiveMaximum { .. } => ("exclusiveMaximum", "number below schema maximum"),
        ExclusiveMinimum { .. } => ("exclusiveMinimum", "number above schema minimum"),
        MultipleOf { .. } => ("multipleOf", "multiple of schema divisor"),
        Pattern { .. } => ("pattern", "string matching schema pattern"),
        PropertyNames { .. } => ("propertyNames", "property names matching schema"),
        UniqueItems => ("uniqueItems", "unique array items"),
        Contains => ("contains", "array containing items matching schema"),
        Not { .. } => ("not", "value outside the excluded schema"),
        FalseSchema => ("falseSchema", "no value is allowed"),
        Format { .. } => ("format", "string matching schema format"),
        ContentEncoding { .. } | ContentMediaType { .. } | FromUtf8 { .. } => {
            ("content", "content matching schema")
        }
        BacktrackLimitExceeded { .. }
        | RegexEngineFailure { .. }
        | Referencing(_)
        | Custom { .. } => (
            "validation_unavailable",
            "schema check could not be completed",
        ),
    };
    (code, expected.to_owned())
}

fn value_type(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn schema_property_names(schema: &Value, names: &mut BTreeSet<String>) {
    match schema {
        Value::Object(object) => {
            if let Some(properties) = object.get("properties").and_then(Value::as_object) {
                names.extend(properties.keys().cloned());
            }
            for value in object.values() {
                schema_property_names(value, names);
            }
        }
        Value::Array(items) => {
            for item in items {
                schema_property_names(item, names);
            }
        }
        _ => {}
    }
}

fn escape(value: &str) -> String {
    value.replace('~', "~0").replace('/', "~1")
}

fn safe_path(pointer: &str, arguments: &Value, names: &BTreeSet<String>) -> String {
    let mut path = "$".to_owned();
    let mut parent = arguments;
    for token in pointer.split('/').skip(1) {
        let key = token.replace("~1", "/").replace("~0", "~");
        path.push('/');
        if parent.is_array() || names.contains(&key) {
            path.push_str(token);
        } else {
            path.push('*');
        }
        parent = parent.pointer(&format!("/{token}")).unwrap_or(&Value::Null);
    }
    path
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn diagnostics(arguments: Value, schema: Value) -> Value {
        let validator = compile(&schema).expect("schema compiles");
        let arguments = crate::argument_normalizer::normalize_builtin_arguments(arguments, &schema)
            .expect("normalization can continue")
            .arguments;
        match validate(&arguments, &schema, &validator).expect_err("invalid arguments") {
            ToolError::InvalidArguments(message) => serde_json::from_str(&message).unwrap(),
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn siblings_and_elements_survive_failed_coercion_and_parent_type_has_no_cascade() {
        let result = diagnostics(
            json!({"parent": 42, "items": [false, 42], "array": "private-secret"}),
            json!({
                "type": "object", "properties": {
                    "parent": {"type": "object", "required": ["child"], "properties": {"child": {"type": "string"}}},
                    "items": {"type": "array", "items": {"type": "string"}},
                    "array": {"type": "array"}
                }
            }),
        );
        assert_eq!(result["diagnostics"].as_array().unwrap().len(), 4);
        let text = result.to_string();
        for path in ["$/parent", "$/items/0", "$/items/1", "$/array"] {
            assert!(text.contains(path));
        }
        assert!(!text.contains("child"));
        assert!(!text.contains("private-secret"));
        assert_eq!(result["validationComplete"], true);
    }

    #[test]
    fn references_and_composites_report_alternatives_once_and_allof_siblings_separately() {
        let schema = json!({
            "$defs": {"choice": {"oneOf": [{"type": "boolean"}, {"type": "integer"}]}},
            "type": "object", "allOf": [
                {"properties": {"a": {"$ref": "#/$defs/choice"}}},
                {"properties": {"b": {"anyOf": [{"type": "string"}, {"type": "null"}]}}},
                {"required": ["c"]}
            ]
        });
        let result = diagnostics(json!({"a": [], "b": false}), schema.clone());
        assert_eq!(result["diagnostics"].as_array().unwrap().len(), 3);
        assert_eq!(result["validationComplete"], true);
        let validator = compile(&schema).unwrap();
        validate(&json!({"a": true, "b": null, "c": 1}), &schema, &validator).unwrap();
    }

    #[test]
    fn nullable_reference_reveals_object_errors_and_wrong_parent_has_no_cascade() {
        let schema = json!({"type": "object", "$defs": {"object": {
            "type": "object", "required": ["a", "b"], "properties": {
                "a": {"type": "string"}, "b": {"type": "array"}
            }
        }}, "properties": {"value": {"anyOf": [{"$ref": "#/$defs/object"}, {"type": "null"}]}}});
        let result = diagnostics(json!({"value": {"a": false, "b": 42}}), schema.clone());
        assert_eq!(result["diagnostics"].as_array().unwrap().len(), 2);
        assert_eq!(result["diagnostics"][0]["path"], "$/value/a");
        assert_eq!(result["diagnostics"][1]["path"], "$/value/b");
        assert_eq!(result["diagnostics"][0]["expected"], "string");
        assert_eq!(result["diagnostics"][1]["expected"], "array");
        let result = diagnostics(json!({"value": false}), schema.clone());
        assert_eq!(result["diagnostics"].as_array().unwrap().len(), 1);
        assert_eq!(result["diagnostics"][0]["code"], "anyOf");
        assert!(!result.to_string().contains("$/value/a"));
        validate(&json!({"value": null}), &schema, &compile(&schema).unwrap()).unwrap();
    }

    #[test]
    fn parent_type_selects_nullable_object_even_when_its_enum_content_is_invalid() {
        let result = diagnostics(
            json!({"kind": "wrong", "text": false}),
            json!({"anyOf": [
                {"type": "object", "properties": {"kind": {"enum": ["allowed"]}, "text": {"type": "string"}}},
                {"type": "null"}
            ]}),
        );
        assert_eq!(result["diagnostics"].as_array().unwrap().len(), 2);
        assert_eq!(result["diagnostics"][0]["path"], "$/kind");
        assert_eq!(result["diagnostics"][1]["path"], "$/text");
    }

    #[test]
    fn literal_tag_selects_variant_without_hiding_its_other_enum_content_errors() {
        let result = diagnostics(
            json!({"tag": "a", "kind": "wrong", "text": false}),
            json!({"oneOf": [
                {"type": "object", "properties": {"tag": {"const": "a"}, "kind": {"enum": ["allowed"]}, "text": {"type": "string"}}},
                {"type": "object", "properties": {"tag": {"const": "b"}, "kind": {"enum": ["allowed"]}}}
            ]}),
        );
        assert_eq!(result["diagnostics"].as_array().unwrap().len(), 2);
        assert_eq!(result["diagnostics"][0]["path"], "$/kind");
        assert_eq!(result["diagnostics"][1]["path"], "$/text");
        let result = diagnostics(
            json!({"x": "a", "y": "b"}),
            json!({"oneOf": [
                {"type": "object", "properties": {"x": {"const": "a"}, "y": {"const": "a"}}},
                {"type": "object", "properties": {"x": {"const": "b"}, "y": {"const": "b"}}}
            ]}),
        );
        assert_eq!(result["diagnostics"].as_array().unwrap().len(), 1);
        assert_eq!(result["diagnostics"][0]["code"], "oneOf");
    }

    #[test]
    fn nested_selected_alternatives_follow_escaped_local_references() {
        let schema = json!({"$defs": {
            "branch/name": {"type": "object", "properties": {"nested": {"anyOf": [
                {"$ref": "#/$defs/leaf"}, {"type": "null"}
            ]}}},
            "leaf": {"type": "object", "properties": {"a": {"type": "string"}, "b": {"type": "array"}}}
        }, "oneOf": [{"$ref": "#/$defs/branch~1name"}, {"type": "boolean"}]});
        let result = diagnostics(json!({"nested": {"a": false, "b": 42}}), schema);
        assert_eq!(result["diagnostics"].as_array().unwrap().len(), 2);
        assert_eq!(result["diagnostics"][0]["path"], "$/nested/a");
        assert_eq!(result["diagnostics"][1]["path"], "$/nested/b");
        assert_eq!(result["validationComplete"], true);
    }

    #[test]
    fn selected_alternative_details_keep_the_display_limit_and_honest_counts() {
        let result = diagnostics(
            json!({"items": vec![false; 40]}),
            json!({"anyOf": [
                {"type": "object", "properties": {"items": {"type": "array", "items": {"type": "string"}}}},
                {"type": "null"}
            ]}),
        );
        assert_eq!(
            result["diagnostics"].as_array().unwrap().len(),
            MAX_DIAGNOSTICS
        );
        assert_eq!(result["hiddenDiagnosticCount"], 8);
        assert_eq!(result["validationComplete"], true);
        assert_eq!(result["diagnosticsTruncated"], true);
        assert_eq!(result["compositeDetailsTruncated"], false);
    }

    #[test]
    fn overlapping_object_alternatives_do_not_choose_the_branch_with_fewer_errors() {
        for keyword in ["oneOf", "anyOf"] {
            let schema = json!({keyword: [
                {"type": "object", "properties": {"a": {"type": "string"}, "b": {"type": "array"}}},
                {"type": "object", "properties": {"a": {"type": "string"}}}
            ]});
            let result = diagnostics(json!({"a": false, "b": 42}), schema);
            assert_eq!(result["diagnostics"].as_array().unwrap().len(), 1);
            assert_eq!(result["diagnostics"][0]["path"], "$");
            assert_eq!(result["diagnostics"][0]["code"], keyword);
        }
    }

    #[test]
    fn completed_validation_reports_hidden_diagnostics_and_redacts_dynamic_keys() {
        let schema = json!({"type": "object", "additionalProperties": {"type": "integer"}});
        let arguments = Value::Object(
            (0..40)
                .map(|index| (format!("secret-{index}"), json!(false)))
                .collect(),
        );
        let result = diagnostics(arguments, schema);
        assert_eq!(
            result["diagnostics"].as_array().unwrap().len(),
            MAX_DIAGNOSTICS
        );
        assert_eq!(result["hiddenDiagnosticCount"], 8);
        assert_eq!(result["diagnosticsTruncated"], true);
        assert_eq!(result["validationComplete"], true);
        assert!(!result.to_string().contains("secret-"));
    }

    #[test]
    fn supported_normalization_and_schemas_without_implied_requirements_remain_valid() {
        let schema = json!({"type": "object", "properties": {
            "array": {"type": "array", "items": {"type": "object"}},
            "ambiguous": {"anyOf": [{"type": "string"}, {"type": "object"}]},
            "optional": {"type": "string"}
        }});
        let arguments = json!({"array": "[{}]", "ambiguous": "{}"});
        let legacy =
            crate::normalize_tool_arguments_from_schema(arguments.clone(), &schema).unwrap();
        let normalized =
            crate::argument_normalizer::normalize_builtin_arguments(arguments, &schema).unwrap();
        assert_eq!(legacy, normalized);
        validate(&normalized.arguments, &schema, &compile(&schema).unwrap()).unwrap();
    }

    #[test]
    fn independent_successful_coercion_is_retained_but_request_is_rejected() {
        let schema = json!({"type": "object", "properties": {
            "a": {"type": "array"}, "b": {"type": "array"}
        }});
        let normalized = crate::argument_normalizer::normalize_builtin_arguments(
            json!({"a": "invalid", "b": "[]"}),
            &schema,
        )
        .unwrap();
        assert_eq!(normalized.arguments["b"], json!([]));
        assert_eq!(normalized.coercions.len(), 1);
        assert!(validate(&normalized.arguments, &schema, &compile(&schema).unwrap()).is_err());
    }

    #[test]
    fn builtin_catalog_schemas_compile_and_declared_constraints_are_checked() {
        for spec in crate::builtin_tool_specs() {
            if spec.spec.payload_kind != crate::PayloadKind::Custom {
                compile(&spec.spec.parameters).expect("builtin schema compiles");
            }
        }
        let result = diagnostics(
            json!({"a": [], "b": "x", "c": 5, "d": "private-value"}),
            json!({
                "type": "object", "required": ["missing"], "properties": {
                    "a": {"type": "array", "minItems": 1},
                    "b": {"type": "string", "minLength": 3},
                    "c": {"type": "number", "maximum": 4},
                    "d": {"enum": ["allowed"]}
                }
            }),
        );
        let codes = result["diagnostics"]
            .as_array()
            .unwrap()
            .iter()
            .map(|d| d["code"].as_str().unwrap())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            codes,
            ["minItems", "minLength", "maximum", "enum", "required"]
                .into_iter()
                .collect()
        );
        assert!(!result.to_string().contains("private-value"));
        let result = diagnostics(
            json!({"private-key": "secret", "another-key": false}),
            json!({"type": "object", "additionalProperties": false}),
        );
        assert_eq!(result["diagnostics"].as_array().unwrap().len(), 2);
        assert!(!result.to_string().contains("private-key"));
        assert!(!result.to_string().contains("secret"));
    }

    #[test]
    fn forbidden_properties_expand_through_references_and_selected_alternatives() {
        let schema = json!({
            "$defs": {"empty/object": {"type": "object", "additionalProperties": false}},
            "type": "object", "properties": {
                "items": {"type": "array", "items": {"anyOf": [
                    {"$ref": "#/$defs/empty~1object"}, {"type": "null"}
                ]}}
            }
        });
        let result = diagnostics(
            json!({"items": [{"private-a": "secret", "private-b": false}]}),
            schema.clone(),
        );
        let entries = result["diagnostics"].as_array().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0]["actualType"], "string");
        assert_eq!(entries[1]["actualType"], "boolean");
        for entry in entries {
            assert_eq!(entry["code"], "additionalProperties");
            assert_eq!(entry["path"], "$/items/0/*");
        }
        assert_eq!(result["validationComplete"], true);
        assert!(!result.to_string().contains("private-"));
        assert!(!result.to_string().contains("secret"));
        validate(
            &json!({"items": [{}, null]}),
            &schema,
            &compile(&schema).unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn genuine_false_schemas_are_not_expanded_into_property_errors() {
        for (arguments, schema, path) in [
            (json!({"a": 1, "b": 2}), json!(false), "$"),
            (
                json!({"additionalProperties": {"a": 1, "b": 2}}),
                json!({"properties": {"additionalProperties": false}}),
                "$/additionalProperties",
            ),
        ] {
            let result = diagnostics(arguments, schema);
            let entries = result["diagnostics"].as_array().unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0]["code"], "falseSchema");
            assert_eq!(entries[0]["path"], path);
            assert_eq!(entries[0]["actualType"], "object");
        }
    }

    #[test]
    fn forbidden_properties_keep_display_limits_and_hidden_counts() {
        let arguments = Value::Object(
            (0..40)
                .map(|index| (format!("private-{index}"), json!(false)))
                .collect(),
        );
        let result = diagnostics(
            arguments,
            json!({"type": "object", "additionalProperties": false}),
        );
        let entries = result["diagnostics"].as_array().unwrap();
        assert_eq!(entries.len(), MAX_DIAGNOSTICS);
        assert!(
            entries
                .iter()
                .all(|entry| entry["code"] == "additionalProperties")
        );
        assert_eq!(result["hiddenDiagnosticCount"], 8);
        assert_eq!(result["diagnosticsTruncated"], true);
        assert_eq!(result["validationComplete"], true);
        assert!(!result.to_string().contains("private-"));
    }

    #[test]
    fn response_byte_limit_and_unavailable_schema_are_explicit() {
        let name = "a".repeat(MAX_DIAGNOSTIC_BYTES);
        let result = diagnostics(json!({}), json!({"type": "object", "required": [name]}));
        assert!(result["diagnostics"].as_array().unwrap().is_empty());
        assert_eq!(result["diagnosticsTruncated"], true);
        assert_eq!(result["validationComplete"], true);
        assert!(compile(&json!({"$ref": "https://invalid.example/schema"})).is_err());
    }
}
