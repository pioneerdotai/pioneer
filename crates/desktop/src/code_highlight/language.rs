const MAX_LANGUAGE_HINT_BYTES: usize = 64;
const LANGUAGE_PREFIX: &str = "language-";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CanonicalLanguage {
    Plaintext,
    Known(&'static str),
    Unknown,
}

impl CanonicalLanguage {
    pub(crate) const fn cache_name(self) -> &'static str {
        match self {
            Self::Plaintext => "plaintext",
            Self::Known(language) => language,
            Self::Unknown => "unknown",
        }
    }

    pub(crate) fn syntax_token(self) -> Option<&'static str> {
        let Self::Known(language) = self else {
            return None;
        };
        Some(match language {
            "shellscript" => "sh",
            "javascript" => "js",
            "typescript" => "ts",
            "rust" => "rs",
            "python" => "py",
            "ruby" => "rb",
            "kotlin" => "kt",
            "cpp" => "cpp",
            "csharp" => "cs",
            "html" => "html",
            "jsonc" => "json",
            "yaml" => "yaml",
            "markdown" => "md",
            "dockerfile" => "Dockerfile",
            language => language,
        })
    }
}

pub(crate) fn normalize_language_hint(hint: Option<&str>) -> CanonicalLanguage {
    let Some(hint) = hint else {
        return CanonicalLanguage::Plaintext;
    };
    let Some(token) = hint
        .trim_matches(|character: char| character.is_ascii_whitespace())
        .split_ascii_whitespace()
        .next()
    else {
        return CanonicalLanguage::Plaintext;
    };
    if token.len() > MAX_LANGUAGE_HINT_BYTES + LANGUAGE_PREFIX.len() {
        return CanonicalLanguage::Unknown;
    }
    let token = token.to_ascii_lowercase();
    let token = token
        .strip_prefix(LANGUAGE_PREFIX)
        .unwrap_or(token.as_str());
    if token.is_empty() {
        return CanonicalLanguage::Plaintext;
    }
    if token.len() > MAX_LANGUAGE_HINT_BYTES {
        return CanonicalLanguage::Unknown;
    }

    match token {
        "text" | "txt" | "plaintext" => CanonicalLanguage::Plaintext,
        "sh" | "shell" | "bash" | "zsh" => CanonicalLanguage::Known("shellscript"),
        "js" | "javascript" | "node" => CanonicalLanguage::Known("javascript"),
        "jsx" => CanonicalLanguage::Known("jsx"),
        "ts" | "typescript" => CanonicalLanguage::Known("typescript"),
        "tsx" => CanonicalLanguage::Known("tsx"),
        "rs" | "rust" => CanonicalLanguage::Known("rust"),
        "py" | "python" => CanonicalLanguage::Known("python"),
        "go" | "golang" => CanonicalLanguage::Known("go"),
        "rb" | "ruby" => CanonicalLanguage::Known("ruby"),
        "java" => CanonicalLanguage::Known("java"),
        "kt" | "kts" | "kotlin" => CanonicalLanguage::Known("kotlin"),
        "swift" => CanonicalLanguage::Known("swift"),
        "c" => CanonicalLanguage::Known("c"),
        "cc" | "cpp" | "c++" | "cxx" | "hpp" => CanonicalLanguage::Known("cpp"),
        "cs" | "c#" | "csharp" => CanonicalLanguage::Known("csharp"),
        "html" | "htm" => CanonicalLanguage::Known("html"),
        "css" => CanonicalLanguage::Known("css"),
        "scss" => CanonicalLanguage::Known("scss"),
        "json" => CanonicalLanguage::Known("json"),
        "jsonc" | "json5" => CanonicalLanguage::Known("jsonc"),
        "yaml" | "yml" => CanonicalLanguage::Known("yaml"),
        "toml" => CanonicalLanguage::Known("toml"),
        "md" | "markdown" => CanonicalLanguage::Known("markdown"),
        "sql" => CanonicalLanguage::Known("sql"),
        "graphql" | "gql" => CanonicalLanguage::Known("graphql"),
        "docker" | "dockerfile" => CanonicalLanguage::Known("dockerfile"),
        "diff" | "patch" => CanonicalLanguage::Known("diff"),
        _ => CanonicalLanguage::Unknown,
    }
}
