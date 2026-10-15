//! Stable desktop opener identity and labels. Native discovery and launch live in the shell.
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FileOpenerId {
    Cursor,
    Trae,
    Kiro,
    #[serde(rename = "vscode")]
    VisualStudioCode,
    #[serde(rename = "vscode-insiders")]
    VisualStudioCodeInsiders,
    Vscodium,
    Zed,
    Antigravity,
    #[serde(rename = "intellij")]
    IntelliJIdea,
    Aqua,
    Clion,
    Datagrip,
    Dataspell,
    Goland,
    Phpstorm,
    Pycharm,
    Rider,
    Rubymine,
    Rustrover,
    Webstorm,
    #[default]
    FileManager,
}

impl FileOpenerId {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Cursor => "Cursor",
            Self::Trae => "Trae",
            Self::Kiro => "Kiro",
            Self::VisualStudioCode => "VS Code",
            Self::VisualStudioCodeInsiders => "VS Code Insiders",
            Self::Vscodium => "VSCodium",
            Self::Zed => "Zed",
            Self::Antigravity => "Antigravity",
            Self::IntelliJIdea => "IntelliJ IDEA",
            Self::Aqua => "Aqua",
            Self::Clion => "CLion",
            Self::Datagrip => "DataGrip",
            Self::Dataspell => "DataSpell",
            Self::Goland => "GoLand",
            Self::Phpstorm => "PhpStorm",
            Self::Pycharm => "PyCharm",
            Self::Rider => "Rider",
            Self::Rubymine => "RubyMine",
            Self::Rustrover => "RustRover",
            Self::Webstorm => "WebStorm",
            Self::FileManager => file_manager_label(),
        }
    }

    pub const fn logo_path(self) -> Option<&'static str> {
        match self {
            Self::Cursor => Some("logos/editors/cursor.svg"),
            Self::Trae => Some("logos/editors/trae.svg"),
            Self::Kiro => Some("logos/editors/kiro.svg"),
            Self::VisualStudioCode => Some("logos/editors/vscode.svg"),
            Self::VisualStudioCodeInsiders => Some("logos/editors/vscode-insiders.svg"),
            Self::Vscodium => Some("logos/editors/vscodium.svg"),
            Self::Zed => Some("logos/editors/zed.svg"),
            Self::Antigravity => Some("logos/editors/antigravity.svg"),
            Self::IntelliJIdea => Some("logos/editors/intellij-idea.svg"),
            Self::Aqua => Some("logos/editors/aqua.svg"),
            Self::Clion => Some("logos/editors/clion.svg"),
            Self::Datagrip => Some("logos/editors/datagrip.svg"),
            Self::Dataspell => Some("logos/editors/dataspell.svg"),
            Self::Goland => Some("logos/editors/goland.svg"),
            Self::Phpstorm => Some("logos/editors/phpstorm.svg"),
            Self::Pycharm => Some("logos/editors/pycharm.svg"),
            Self::Rider => Some("logos/editors/rider.svg"),
            Self::Rubymine => Some("logos/editors/rubymine.svg"),
            Self::Rustrover => Some("logos/editors/rustrover.svg"),
            Self::Webstorm => Some("logos/editors/webstorm.svg"),
            Self::FileManager => Some(file_manager_logo_path()),
        }
    }
}
#[cfg(target_os = "macos")]
const fn file_manager_label() -> &'static str {
    "Finder"
}
#[cfg(target_os = "macos")]
const fn file_manager_logo_path() -> &'static str {
    "logos/editors/finder.svg"
}
#[cfg(windows)]
const fn file_manager_label() -> &'static str {
    "Explorer"
}

#[cfg(windows)]
const fn file_manager_logo_path() -> &'static str {
    "logos/editors/explorer.svg"
}

#[cfg(all(not(target_os = "macos"), not(windows)))]
const fn file_manager_label() -> &'static str {
    "Files"
}

#[cfg(all(not(target_os = "macos"), not(windows)))]
const fn file_manager_logo_path() -> &'static str {
    "logos/editors/files.svg"
}
