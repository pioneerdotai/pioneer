use super::*;

const LIMITS: HighlightLimits = HighlightLimits::DESKTOP;

fn highlighted(source: &str, language: &str, theme: CodeThemeId) -> HighlightedCode {
    match highlight_code(source, Some(language), theme, LIMITS).expect("highlight outcome") {
        HighlightOutcome::Highlighted(code) => code,
        HighlightOutcome::Fallback(reason) => panic!("unexpected fallback: {reason:?}"),
    }
}

fn assert_source_ranges(source: &str, code: &HighlightedCode) {
    let mut cursor = 0usize;
    for span in &code.spans {
        assert_eq!(span.byte_range.start, cursor);
        assert!(span.byte_range.start < span.byte_range.end);
        assert!(source.is_char_boundary(span.byte_range.start));
        assert!(source.is_char_boundary(span.byte_range.end));
        let _ = &source[span.byte_range.clone()];
        if let Some(next) = code
            .spans
            .iter()
            .find(|candidate| candidate.byte_range.start == span.byte_range.end)
        {
            assert_ne!(span.foreground, next.foreground);
        }
        cursor = span.byte_range.end;
    }
    assert_eq!(cursor, source.len());
}

fn source_with_size(size: usize) -> String {
    const LINE: &str = "let value: &str = \"timeline syntax highlighting\";\n";
    let mut source = LINE.repeat(size.div_ceil(LINE.len()));
    source.truncate(size);
    source
}

#[test]
fn canonical_aliases_match_the_normative_registry() {
    let groups: &[(&[&str], CanonicalLanguage)] = &[
        (
            &["text", "txt", "plaintext", ""],
            CanonicalLanguage::Plaintext,
        ),
        (
            &["sh", "shell", "bash", "zsh"],
            CanonicalLanguage::Known("shellscript"),
        ),
        (
            &["js", "javascript", "node"],
            CanonicalLanguage::Known("javascript"),
        ),
        (&["jsx"], CanonicalLanguage::Known("jsx")),
        (
            &["ts", "typescript"],
            CanonicalLanguage::Known("typescript"),
        ),
        (&["tsx"], CanonicalLanguage::Known("tsx")),
        (&["rs", "rust"], CanonicalLanguage::Known("rust")),
        (&["py", "python"], CanonicalLanguage::Known("python")),
        (&["go", "golang"], CanonicalLanguage::Known("go")),
        (&["rb", "ruby"], CanonicalLanguage::Known("ruby")),
        (&["java"], CanonicalLanguage::Known("java")),
        (&["kt", "kts", "kotlin"], CanonicalLanguage::Known("kotlin")),
        (&["swift"], CanonicalLanguage::Known("swift")),
        (&["c"], CanonicalLanguage::Known("c")),
        (
            &["cc", "cpp", "c++", "cxx", "hpp"],
            CanonicalLanguage::Known("cpp"),
        ),
        (&["cs", "c#", "csharp"], CanonicalLanguage::Known("csharp")),
        (&["html", "htm"], CanonicalLanguage::Known("html")),
        (&["css"], CanonicalLanguage::Known("css")),
        (&["scss"], CanonicalLanguage::Known("scss")),
        (&["json"], CanonicalLanguage::Known("json")),
        (&["jsonc", "json5"], CanonicalLanguage::Known("jsonc")),
        (&["yaml", "yml"], CanonicalLanguage::Known("yaml")),
        (&["toml"], CanonicalLanguage::Known("toml")),
        (&["md", "markdown"], CanonicalLanguage::Known("markdown")),
        (&["sql"], CanonicalLanguage::Known("sql")),
        (&["graphql", "gql"], CanonicalLanguage::Known("graphql")),
        (
            &["docker", "dockerfile"],
            CanonicalLanguage::Known("dockerfile"),
        ),
        (&["diff", "patch"], CanonicalLanguage::Known("diff")),
    ];
    for (aliases, expected) in groups {
        for alias in *aliases {
            assert_eq!(normalize_language_hint(Some(alias)), *expected, "{alias}");
        }
    }
    assert_eq!(
        normalize_language_hint(Some("  language-BASH metadata ")),
        CanonicalLanguage::Known("shellscript")
    );
}

#[test]
fn empty_missing_plaintext_and_unknown_hints_fall_back() {
    assert_eq!(
        highlight_code("", Some("rust"), CodeThemeId::Dark, LIMITS).unwrap(),
        HighlightOutcome::Fallback(HighlightFallbackReason::Empty)
    );
    assert_eq!(
        highlight_code("code", None, CodeThemeId::Dark, LIMITS).unwrap(),
        HighlightOutcome::Fallback(HighlightFallbackReason::Plaintext)
    );
    assert_eq!(
        highlight_code("code", Some("txt"), CodeThemeId::Dark, LIMITS).unwrap(),
        HighlightOutcome::Fallback(HighlightFallbackReason::Plaintext)
    );
    assert_eq!(
        highlight_code("code", Some("made-up"), CodeThemeId::Dark, LIMITS).unwrap(),
        HighlightOutcome::Fallback(HighlightFallbackReason::UnknownLanguage)
    );
    assert_eq!(
        normalize_language_hint(Some(&"x".repeat(65))),
        CanonicalLanguage::Unknown
    );
}

#[test]
fn multiline_language_state_and_unicode_ranges_are_preserved() {
    let fixtures = [
        ("rust", include_str!("fixtures/multiline.rs")),
        ("javascript", include_str!("fixtures/template.js")),
        ("python", include_str!("fixtures/multiline.py")),
        ("bash", include_str!("fixtures/heredoc.sh")),
    ];
    for (language, source) in fixtures {
        let code = match highlight_code(source, Some(language), CodeThemeId::Dark, LIMITS)
            .expect("highlight outcome")
        {
            HighlightOutcome::Highlighted(code) => code,
            HighlightOutcome::Fallback(reason) => {
                panic!("{language} unexpectedly fell back: {reason:?}")
            }
        };
        assert_source_ranges(source, &code);
    }
}

#[test]
fn every_canonical_language_resolves_an_embedded_syntax() {
    let fixtures = [
        ("bash", "echo hello\n"),
        ("javascript", "const value = 1;\n"),
        ("jsx", "const value = <div />;\n"),
        ("typescript", "const value: number = 1;\n"),
        ("tsx", "const value: JSX.Element = <div />;\n"),
        ("rust", "let value: u8 = 1;\n"),
        ("python", "value = 1\n"),
        ("go", "package main\n"),
        ("ruby", "value = 1\n"),
        ("java", "class Main {}\n"),
        ("kotlin", "val value = 1\n"),
        ("swift", "let value = 1\n"),
        ("c", "int value = 1;\n"),
        ("cpp", "auto value = 1;\n"),
        ("csharp", "var value = 1;\n"),
        ("html", "<div></div>\n"),
        ("css", ".value { color: red; }\n"),
        ("scss", "$value: red;\n"),
        ("json", "{\"value\": 1}\n"),
        ("jsonc", "{\"value\": 1} // comment\n"),
        ("yaml", "value: 1\n"),
        ("toml", "value = 1\n"),
        ("markdown", "# Heading\n"),
        ("sql", "SELECT 1;\n"),
        ("graphql", "query { value }\n"),
        ("dockerfile", "FROM scratch\n"),
        ("diff", "+value\n"),
    ];
    for (language, source) in fixtures {
        let code = match highlight_code(source, Some(language), CodeThemeId::Dark, LIMITS)
            .expect("highlight outcome")
        {
            HighlightOutcome::Highlighted(code) => code,
            HighlightOutcome::Fallback(reason) => {
                panic!("{language} unexpectedly fell back: {reason:?}")
            }
        };
        assert_source_ranges(source, &code);
    }
}

#[test]
fn unusual_valid_rust_strings_never_panic_or_change_source() {
    let source = "\0\u{202e}abc\u{2066}\t👩🏽‍💻 中文 e\u{301}\r\n";
    let code = highlighted(source, "rust", CodeThemeId::Dark);
    assert_source_ranges(source, &code);
}

#[test]
fn source_and_span_caps_return_permanent_fallbacks() {
    let source_limited = HighlightLimits::new(3, 100);
    assert_eq!(
        highlight_code("four", Some("rust"), CodeThemeId::Dark, source_limited).unwrap(),
        HighlightOutcome::Fallback(HighlightFallbackReason::SourceTooLarge)
    );

    let span_limited = HighlightLimits::new(1024, 1);
    assert_eq!(
        highlight_code(
            "fn main() { let value = 1; }",
            Some("rust"),
            CodeThemeId::Dark,
            span_limited,
        )
        .unwrap(),
        HighlightOutcome::Fallback(HighlightFallbackReason::SpanLimit)
    );
}

#[test]
fn cache_keys_are_deterministic_and_separate_language_and_theme() {
    let rust = normalize_language_hint(Some("rs"));
    assert_eq!(
        make_highlight_key("source", rust, CodeThemeId::Dark),
        make_highlight_key("source", rust, CodeThemeId::Dark)
    );
    assert_ne!(
        make_highlight_key("source", rust, CodeThemeId::Dark),
        make_highlight_key("source", rust, CodeThemeId::Light)
    );
    assert_ne!(
        make_highlight_key("source", rust, CodeThemeId::Dark),
        make_highlight_key(
            "source",
            normalize_language_hint(Some("ts")),
            CodeThemeId::Dark,
        )
    );
}

#[test]
fn light_and_dark_themes_produce_different_foregrounds() {
    let source = "fn main() { println!(\"hello\"); }\n";
    let light = highlighted(source, "rust", CodeThemeId::Light);
    let dark = highlighted(source, "rust", CodeThemeId::Dark);
    assert_source_ranges(source, &light);
    assert_source_ranges(source, &dark);
    assert_ne!(light.spans, dark.spans);
    assert!(super::cache::estimated_payload_bytes(&dark) >= source.len());
}

#[test]
fn desktop_qa_language_theme_and_content_matrix_preserves_source() {
    let fixtures = [
        (
            "rust",
            "/* multi\nline */\nlet tabbed = \"👩🏽‍💻 中文 é\";\n\tprintln!(\"{tabbed}\");\n",
        ),
        (
            "typescript",
            "type Item = { value: string };\nconst item: Item = { value: `hello ${1}` };\n",
        ),
        (
            "tsx",
            "export const View = () => <section data-value=\"x\">Hello</section>;\n",
        ),
        (
            "python",
            "\"\"\"multi\nline\"\"\"\nvalue = f\"hello {1}\"\n",
        ),
        (
            "bash",
            "cat <<'EOF'\nhello $USER\nEOF\nprintf '%s\\n' \"done\"\n",
        ),
        ("json", "{\"emoji\": \"👩🏽‍💻\", \"enabled\": true}\n"),
        ("yaml", "name: pioneer\nitems:\n  - one\n  - two\n"),
        ("markdown", "# Heading\n\n`inline` and **strong**\n"),
    ];
    for theme in [CodeThemeId::Light, CodeThemeId::Dark] {
        for (language, source) in fixtures {
            let code = highlighted(source, language, theme);
            assert_source_ranges(source, &code);
        }
        assert_eq!(
            highlight_code("unknown source", Some("not-a-language"), theme, LIMITS).unwrap(),
            HighlightOutcome::Fallback(HighlightFallbackReason::UnknownLanguage)
        );
    }
}

#[test]
fn desktop_qa_size_matrix_highlights_within_cap_and_falls_back_above_it() {
    let one_line = "let value = 1;\n";
    let one_hundred_lines = "let value = 1;\n".repeat(100);
    let ten_kib = source_with_size(10 * 1024);
    let one_hundred_kib = source_with_size(100 * 1024);

    for theme in [CodeThemeId::Light, CodeThemeId::Dark] {
        assert_eq!(
            highlight_code("", Some("rust"), theme, LIMITS).unwrap(),
            HighlightOutcome::Fallback(HighlightFallbackReason::Empty)
        );
        for source in [
            one_line,
            one_hundred_lines.as_str(),
            ten_kib.as_str(),
            one_hundred_kib.as_str(),
        ] {
            let code = highlighted(source, "rust", theme);
            assert_source_ranges(source, &code);
        }
        let oversized = source_with_size(HighlightLimits::DESKTOP.max_source_bytes + 1);
        assert_eq!(
            highlight_code(oversized.as_str(), Some("rust"), theme, LIMITS).unwrap(),
            HighlightOutcome::Fallback(HighlightFallbackReason::SourceTooLarge)
        );
    }
}

#[test]
fn desktop_theme_token_colors_keep_readable_contrast() {
    let source = "fn main() { let message = \"hello\"; println!(\"{message}\"); }\n";
    for theme_id in [CodeThemeId::Light, CodeThemeId::Dark] {
        let background = super::theme::render_background(theme_id);
        let code = highlighted(source, "rust", theme_id);
        for span in code.spans {
            let contrast = contrast_ratio(span.foreground, background);
            assert!(
                contrast >= 3.0,
                "{theme_id:?} token {:?} has contrast {contrast:.2}",
                span.foreground
            );
        }
    }
}

fn contrast_ratio(foreground: Rgba8, background: syntect::highlighting::Color) -> f64 {
    let foreground = relative_luminance(foreground.red, foreground.green, foreground.blue);
    let background = relative_luminance(background.r, background.g, background.b);
    let (lighter, darker) = if foreground >= background {
        (foreground, background)
    } else {
        (background, foreground)
    };
    (lighter + 0.05) / (darker + 0.05)
}

fn relative_luminance(red: u8, green: u8, blue: u8) -> f64 {
    fn channel(value: u8) -> f64 {
        let value = f64::from(value) / 255.0;
        if value <= 0.04045 {
            value / 12.92
        } else {
            ((value + 0.055) / 1.055).powf(2.4)
        }
    }
    0.2126 * channel(red) + 0.7152 * channel(green) + 0.0722 * channel(blue)
}

#[test]
fn cache_schedules_once_and_applies_ready_result() {
    let mut cache = DesktopCodeHighlightCache::default();
    let first = cache.request("let value = 1;", Some("rust"), CodeThemeId::Dark, LIMITS);
    assert_eq!(first.lookup, CodeHighlightLookup::Pending);
    assert_eq!(first.jobs.len(), 1);
    let repeated = cache.request("let value = 1;", Some("rs"), CodeThemeId::Dark, LIMITS);
    assert_eq!(repeated.lookup, CodeHighlightLookup::Pending);
    assert!(repeated.jobs.is_empty());
    assert!(!repeated.observe_cache_hit);

    let job = &first.jobs[0];
    let result = highlight_code(
        job.source.as_ref(),
        job.language_hint.as_deref(),
        job.theme,
        job.limits,
    );
    let completion = cache.complete(&job.key, job.generation, result);
    assert!(completion.visible_output_changed);
    assert!(completion.jobs.is_empty());

    let ready = cache.request("let value = 1;", Some("rust"), CodeThemeId::Dark, LIMITS);
    assert!(matches!(ready.lookup, CodeHighlightLookup::Ready(_)));
    assert!(ready.jobs.is_empty());
    assert!(ready.observe_cache_hit);
    assert!(
        !cache
            .request("let value = 1;", Some("rust"), CodeThemeId::Dark, LIMITS)
            .observe_cache_hit
    );
}

#[test]
fn cache_stores_permanent_fallback_and_ignores_stale_completion() {
    let mut cache = DesktopCodeHighlightCache::default();
    let fallback = cache.request("plain", Some("unknown"), CodeThemeId::Dark, LIMITS);
    assert_eq!(
        fallback.lookup,
        CodeHighlightLookup::Fallback(HighlightFallbackReason::UnknownLanguage)
    );
    assert!(fallback.jobs.is_empty());
    assert!(
        cache
            .request("plain", Some("unknown"), CodeThemeId::Dark, LIMITS)
            .jobs
            .is_empty()
    );

    let pending = cache.request("let value = 1;", Some("rust"), CodeThemeId::Dark, LIMITS);
    let job = &pending.jobs[0];
    let stale = cache.complete(
        &job.key,
        job.generation + 100,
        Ok(HighlightOutcome::Fallback(
            HighlightFallbackReason::ParserError,
        )),
    );
    assert!(!stale.visible_output_changed);
    assert_eq!(
        cache
            .request("let value = 1;", Some("rust"), CodeThemeId::Dark, LIMITS)
            .lookup,
        CodeHighlightLookup::Pending
    );
}

#[test]
fn cache_rejects_oversized_source_before_allocating_an_entry() {
    let mut cache = DesktopCodeHighlightCache::default();
    let limits = HighlightLimits::new(3, 100);
    let request = cache.request("four", Some("rust"), CodeThemeId::Dark, limits);
    assert_eq!(
        request.lookup,
        CodeHighlightLookup::Fallback(HighlightFallbackReason::SourceTooLarge)
    );
    assert!(request.jobs.is_empty());
    assert!(request.observe_immediate_fallback);
    assert_eq!(cache.test_entry_count(), 0);
    assert!(
        !cache
            .request("five", Some("rust"), CodeThemeId::Dark, limits)
            .observe_immediate_fallback
    );
}

#[test]
fn cache_limits_active_jobs_and_starts_queue_on_completion() {
    let mut cache = DesktopCodeHighlightCache::default();
    let mut active = Vec::new();
    for index in 0..5 {
        let request = cache.request(
            format!("let value_{index} = {index};").as_str(),
            Some("rust"),
            CodeThemeId::Dark,
            LIMITS,
        );
        active.extend(request.jobs);
    }
    assert_eq!(active.len(), 4);
    let first = &active[0];
    let completion = cache.complete(
        &first.key,
        first.generation,
        Ok(HighlightOutcome::Fallback(
            HighlightFallbackReason::ParserError,
        )),
    );
    assert!(!completion.visible_output_changed);
    assert_eq!(completion.jobs.len(), 1);
    assert_eq!(completion.jobs[0].source.as_ref(), "let value_4 = 4;");
}

#[test]
fn cache_promotes_a_revisited_queued_job() {
    let mut cache = DesktopCodeHighlightCache::default();
    let mut active = Vec::new();
    for index in 0..6 {
        let request = cache.request(
            format!("let value_{index} = {index};").as_str(),
            Some("rust"),
            CodeThemeId::Dark,
            LIMITS,
        );
        active.extend(request.jobs);
    }
    assert_eq!(active.len(), 4);

    let revisited = cache.request("let value_4 = 4;", Some("rust"), CodeThemeId::Dark, LIMITS);
    assert_eq!(revisited.lookup, CodeHighlightLookup::Pending);
    assert!(revisited.jobs.is_empty());

    let first = &active[0];
    let completion = cache.complete(
        &first.key,
        first.generation,
        Ok(HighlightOutcome::Fallback(
            HighlightFallbackReason::ParserError,
        )),
    );
    assert_eq!(completion.jobs.len(), 1);
    assert_eq!(completion.jobs[0].source.as_ref(), "let value_4 = 4;");
}

#[test]
fn cache_separates_themes_and_enforces_lru_and_payload_caps() {
    let mut themes = DesktopCodeHighlightCache::default();
    let dark = themes.request("let value = 1;", Some("rust"), CodeThemeId::Dark, LIMITS);
    let light = themes.request("let value = 1;", Some("rust"), CodeThemeId::Light, LIMITS);
    assert_eq!(dark.jobs.len(), 1);
    assert_eq!(light.jobs.len(), 1);
    assert_ne!(dark.jobs[0].key, light.jobs[0].key);

    let mut lru = DesktopCodeHighlightCache::test_cache(2, usize::MAX);
    let key_a = make_highlight_key("a", CanonicalLanguage::Plaintext, CodeThemeId::Dark);
    let key_b = make_highlight_key("b", CanonicalLanguage::Plaintext, CodeThemeId::Dark);
    let key_c = make_highlight_key("c", CanonicalLanguage::Plaintext, CodeThemeId::Dark);
    lru.request("a", Some("text"), CodeThemeId::Dark, LIMITS);
    lru.request("b", Some("text"), CodeThemeId::Dark, LIMITS);
    lru.request("a", Some("text"), CodeThemeId::Dark, LIMITS);
    lru.request("c", Some("text"), CodeThemeId::Dark, LIMITS);
    assert_eq!(lru.test_entry_count(), 2);
    assert!(lru.test_contains(&key_a));
    assert!(!lru.test_contains(&key_b));
    assert!(lru.test_contains(&key_c));

    let mut payload_limited = DesktopCodeHighlightCache::test_cache(8, 1);
    let request =
        payload_limited.request("let value = 1;", Some("rust"), CodeThemeId::Dark, LIMITS);
    let job = &request.jobs[0];
    let result = highlight_code(
        job.source.as_ref(),
        job.language_hint.as_deref(),
        job.theme,
        job.limits,
    );
    let completion = payload_limited.complete(&job.key, job.generation, result);
    assert!(!completion.visible_output_changed);
    assert_eq!(payload_limited.test_payload_bytes(), 0);
    assert_eq!(
        payload_limited
            .request("let value = 1;", Some("rust"), CodeThemeId::Dark, LIMITS,)
            .lookup,
        CodeHighlightLookup::Fallback(HighlightFallbackReason::SpanLimit)
    );
}
