use anyhow::{Context as _, Result, bail};
use std::{
    env,
    ffi::OsString,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::OnceLock,
};

#[cfg(windows)]
use std::ffi::OsStr;
#[cfg(target_os = "macos")]
use std::{fs::File, io::Read as _};

pub(crate) use pioneer_desktop_settings::FileOpenerId;

pub(crate) const FILE_OPENER_CANDIDATES: [FileOpenerId; 21] = [
    FileOpenerId::Cursor,
    FileOpenerId::Trae,
    FileOpenerId::Kiro,
    FileOpenerId::VisualStudioCode,
    FileOpenerId::VisualStudioCodeInsiders,
    FileOpenerId::Vscodium,
    FileOpenerId::Zed,
    FileOpenerId::Antigravity,
    FileOpenerId::IntelliJIdea,
    FileOpenerId::Aqua,
    FileOpenerId::Clion,
    FileOpenerId::Datagrip,
    FileOpenerId::Dataspell,
    FileOpenerId::Goland,
    FileOpenerId::Phpstorm,
    FileOpenerId::Pycharm,
    FileOpenerId::Rider,
    FileOpenerId::Rubymine,
    FileOpenerId::Rustrover,
    FileOpenerId::Webstorm,
    FileOpenerId::FileManager,
];

#[derive(Debug)]
pub(crate) struct AvailableFileOpener {
    pub(crate) id: FileOpenerId,
    executable: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LocalFileTarget {
    path: PathBuf,
    line: Option<u32>,
    column: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LaunchStyle {
    DirectPath,
    Goto,
    LineColumn,
}

static AVAILABLE_FILE_OPENERS: OnceLock<Vec<AvailableFileOpener>> = OnceLock::new();

trait FileOpenerLaunch {
    fn commands(self) -> &'static [&'static str];
    fn base_args(self) -> &'static [&'static str];
    fn launch_style(self) -> LaunchStyle;
}
impl FileOpenerLaunch for FileOpenerId {
    fn commands(self) -> &'static [&'static str] {
        match self {
            Self::Cursor => &["cursor"],
            Self::Trae => &["trae"],
            Self::Kiro => &["kiro"],
            Self::VisualStudioCode => &["code"],
            Self::VisualStudioCodeInsiders => &["code-insiders"],
            Self::Vscodium => &["codium"],
            Self::Zed => &["zed", "zeditor"],
            Self::Antigravity => &["agy"],
            Self::IntelliJIdea => &["idea"],
            Self::Aqua => &["aqua"],
            Self::Clion => &["clion"],
            Self::Datagrip => &["datagrip"],
            Self::Dataspell => &["dataspell"],
            Self::Goland => &["goland"],
            Self::Phpstorm => &["phpstorm"],
            Self::Pycharm => &["pycharm"],
            Self::Rider => &["rider"],
            Self::Rubymine => &["rubymine"],
            Self::Rustrover => &["rustrover"],
            Self::Webstorm => &["webstorm"],
            Self::FileManager => &[],
        }
    }

    fn base_args(self) -> &'static [&'static str] {
        match self {
            Self::Kiro => &["ide"],
            _ => &[],
        }
    }

    fn launch_style(self) -> LaunchStyle {
        match self {
            Self::Zed | Self::FileManager => LaunchStyle::DirectPath,
            Self::Cursor
            | Self::Trae
            | Self::Kiro
            | Self::VisualStudioCode
            | Self::VisualStudioCodeInsiders
            | Self::Vscodium
            | Self::Antigravity => LaunchStyle::Goto,
            Self::IntelliJIdea
            | Self::Aqua
            | Self::Clion
            | Self::Datagrip
            | Self::Dataspell
            | Self::Goland
            | Self::Phpstorm
            | Self::Pycharm
            | Self::Rider
            | Self::Rubymine
            | Self::Rustrover
            | Self::Webstorm => LaunchStyle::LineColumn,
        }
    }
}

impl LocalFileTarget {
    pub(crate) fn new(path: PathBuf, line: Option<u32>, column: Option<u32>) -> Self {
        Self { path, line, column }
    }

    pub(crate) fn path(&self) -> &Path {
        self.path.as_path()
    }

    fn target_with_position(&self) -> OsString {
        let mut target = self.path.as_os_str().to_os_string();
        if let Some(line) = self.line {
            target.push(format!(":{line}"));
            if let Some(column) = self.column {
                target.push(format!(":{column}"));
            }
        }
        target
    }
}

pub(crate) fn available_file_openers() -> &'static [AvailableFileOpener] {
    AVAILABLE_FILE_OPENERS.get_or_init(discover_file_openers)
}

pub(crate) fn is_file_opener_available(id: FileOpenerId) -> bool {
    available_file_openers()
        .iter()
        .any(|opener| opener.id == id)
}

pub(crate) fn available_or_file_manager(id: FileOpenerId) -> FileOpenerId {
    if is_file_opener_available(id) {
        id
    } else {
        FileOpenerId::FileManager
    }
}

pub(crate) fn open_local_file(opener: FileOpenerId, target: &LocalFileTarget) -> Result<()> {
    if opener == FileOpenerId::FileManager {
        return reveal_in_file_manager(target.path());
    }

    let executable = available_file_openers()
        .iter()
        .find(|available| available.id == opener)
        .and_then(|available| available.executable.as_deref())
        .with_context(|| format!("{} is not available on PATH", opener.label()))?;
    let args = editor_args(opener, target);
    spawn_executable(executable, args.as_slice(), "open local file")
}

fn discover_file_openers() -> Vec<AvailableFileOpener> {
    let search_paths = executable_search_paths();
    let mut available = FILE_OPENER_CANDIDATES
        .iter()
        .copied()
        .filter_map(|id| {
            if id == FileOpenerId::FileManager {
                return None;
            }
            resolve_file_opener_executable(id, search_paths.as_slice()).map(|executable| {
                AvailableFileOpener {
                    id,
                    executable: Some(executable),
                }
            })
        })
        .collect::<Vec<_>>();

    // The platform file manager is the durable fallback and is therefore always
    // advertised. Every supported desktop ships its platform opener.
    available.push(AvailableFileOpener {
        id: FileOpenerId::FileManager,
        executable: None,
    });
    available
}

fn resolve_file_opener_executable(id: FileOpenerId, search_paths: &[PathBuf]) -> Option<PathBuf> {
    resolve_first_command(id.commands(), search_paths).or_else(|| {
        #[cfg(target_os = "macos")]
        {
            resolve_macos_app_executable(id)
        }
        #[cfg(not(target_os = "macos"))]
        {
            None
        }
    })
}

fn executable_search_paths() -> Vec<PathBuf> {
    let mut paths = env::var_os("PATH")
        .map(|path| env::split_paths(&path).collect::<Vec<_>>())
        .unwrap_or_default();

    // GUI applications frequently receive a minimal PATH. These are normal CLI
    // installation locations and mirror the commands that t3code probes.
    for path in ["/usr/local/bin", "/opt/homebrew/bin", "/usr/bin", "/bin"] {
        push_unique_path(&mut paths, PathBuf::from(path));
    }
    if let Some(home) = env::var_os("HOME").map(PathBuf::from) {
        for relative in [
            ".local/bin",
            "Library/Application Support/JetBrains/Toolbox/scripts",
            ".local/share/JetBrains/Toolbox/scripts",
            ".config/JetBrains/Toolbox/scripts",
        ] {
            push_unique_path(&mut paths, home.join(relative));
        }
    }
    paths
}

fn push_unique_path(paths: &mut Vec<PathBuf>, path: PathBuf) {
    if !paths.contains(&path) {
        paths.push(path);
    }
}

fn resolve_first_command(commands: &[&str], search_paths: &[PathBuf]) -> Option<PathBuf> {
    commands.iter().find_map(|command| {
        executable_names(command).into_iter().find_map(|name| {
            search_paths
                .iter()
                .map(|directory| directory.join(name.as_path()))
                .find(|candidate| is_executable(candidate.as_path()))
        })
    })
}

#[cfg(windows)]
fn executable_names(command: &str) -> Vec<PathBuf> {
    if Path::new(command).extension().is_some() {
        return vec![PathBuf::from(command)];
    }
    let extensions = env::var("PATHEXT").unwrap_or_else(|_| ".COM;.EXE;.BAT;.CMD".to_owned());
    extensions
        .split(';')
        .filter(|extension| !extension.is_empty())
        .map(|extension| PathBuf::from(format!("{command}{}", extension.to_ascii_lowercase())))
        .collect()
}

#[cfg(not(windows))]
fn executable_names(command: &str) -> Vec<PathBuf> {
    vec![PathBuf::from(command)]
}

#[cfg(target_os = "macos")]
fn is_executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt as _;

    path.metadata()
        .map(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
        && macos_launcher_target_exists(path)
}

#[cfg(all(unix, not(target_os = "macos")))]
fn is_executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt as _;
    path.metadata()
        .map(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

#[cfg(target_os = "macos")]
fn macos_launcher_target_exists(path: &Path) -> bool {
    let Ok(file) = File::open(path) else {
        return false;
    };
    let mut prefix = Vec::with_capacity(8 * 1024);
    if file.take(8 * 1024).read_to_end(&mut prefix).is_err() || !prefix.starts_with(b"#!") {
        return true;
    }

    let script = String::from_utf8_lossy(&prefix);
    let referenced_app_executables = script
        .split(['"', '\''])
        .map(str::trim)
        .filter(|candidate| {
            candidate.starts_with('/') && candidate.contains(".app/Contents/MacOS/")
        })
        .collect::<Vec<_>>();
    referenced_app_executables.is_empty()
        || referenced_app_executables
            .iter()
            .all(|candidate| Path::new(candidate).is_file())
}

#[cfg(target_os = "macos")]
fn resolve_macos_app_executable(id: FileOpenerId) -> Option<PathBuf> {
    resolve_macos_app_executable_in_roots(id, macos_application_roots().as_slice())
}

#[cfg(target_os = "macos")]
fn resolve_macos_app_executable_in_roots(id: FileOpenerId, roots: &[PathBuf]) -> Option<PathBuf> {
    for root in roots {
        for app_name in macos_app_names(id) {
            let bundle = root.join(app_name);
            if !bundle.is_dir() {
                continue;
            }
            for relative_executable in macos_bundled_executables(id) {
                let executable = bundle.join(relative_executable);
                if is_executable(executable.as_path()) {
                    return Some(executable);
                }
            }
        }
    }
    None
}

#[cfg(target_os = "macos")]
fn macos_application_roots() -> Vec<PathBuf> {
    let mut roots = vec![
        PathBuf::from("/Applications"),
        PathBuf::from("/System/Applications"),
    ];
    if let Some(home) = env::var_os("HOME").map(PathBuf::from) {
        roots.push(home.join("Applications"));
    }
    roots
}

#[cfg(target_os = "macos")]
const fn macos_app_names(id: FileOpenerId) -> &'static [&'static str] {
    match id {
        FileOpenerId::Cursor => &["Cursor.app"],
        FileOpenerId::Trae => &["Trae.app"],
        FileOpenerId::Kiro => &["Kiro.app"],
        FileOpenerId::VisualStudioCode => &["Visual Studio Code.app"],
        FileOpenerId::VisualStudioCodeInsiders => &["Visual Studio Code - Insiders.app"],
        FileOpenerId::Vscodium => &["VSCodium.app"],
        FileOpenerId::Zed => &["Zed.app"],
        FileOpenerId::Antigravity => &["Antigravity.app"],
        FileOpenerId::IntelliJIdea => &[
            "IntelliJ IDEA.app",
            "IntelliJ IDEA Ultimate.app",
            "IntelliJ IDEA Community Edition.app",
        ],
        FileOpenerId::Aqua => &["Aqua.app"],
        FileOpenerId::Clion => &["CLion.app"],
        FileOpenerId::Datagrip => &["DataGrip.app"],
        FileOpenerId::Dataspell => &["DataSpell.app"],
        FileOpenerId::Goland => &["GoLand.app"],
        FileOpenerId::Phpstorm => &["PhpStorm.app"],
        FileOpenerId::Pycharm => &["PyCharm.app"],
        FileOpenerId::Rider => &["Rider.app"],
        FileOpenerId::Rubymine => &["RubyMine.app"],
        FileOpenerId::Rustrover => &["RustRover.app"],
        FileOpenerId::Webstorm => &["WebStorm.app"],
        FileOpenerId::FileManager => &[],
    }
}

#[cfg(target_os = "macos")]
const fn macos_bundled_executables(id: FileOpenerId) -> &'static [&'static str] {
    match id {
        FileOpenerId::Cursor => &["Contents/Resources/app/bin/cursor"],
        FileOpenerId::Trae => &["Contents/Resources/app/bin/trae"],
        FileOpenerId::Kiro => &["Contents/Resources/app/bin/kiro"],
        FileOpenerId::VisualStudioCode => &["Contents/Resources/app/bin/code"],
        FileOpenerId::VisualStudioCodeInsiders => &["Contents/Resources/app/bin/code-insiders"],
        FileOpenerId::Vscodium => &["Contents/Resources/app/bin/codium"],
        FileOpenerId::Zed => &["Contents/MacOS/cli"],
        FileOpenerId::Antigravity => &["Contents/Resources/app/bin/agy"],
        FileOpenerId::IntelliJIdea => &["Contents/MacOS/idea"],
        FileOpenerId::Aqua => &["Contents/MacOS/aqua"],
        FileOpenerId::Clion => &["Contents/MacOS/clion"],
        FileOpenerId::Datagrip => &["Contents/MacOS/datagrip"],
        FileOpenerId::Dataspell => &["Contents/MacOS/dataspell"],
        FileOpenerId::Goland => &["Contents/MacOS/goland"],
        FileOpenerId::Phpstorm => &["Contents/MacOS/phpstorm"],
        FileOpenerId::Pycharm => &["Contents/MacOS/pycharm"],
        FileOpenerId::Rider => &["Contents/MacOS/rider"],
        FileOpenerId::Rubymine => &["Contents/MacOS/rubymine"],
        FileOpenerId::Rustrover => &["Contents/MacOS/rustrover"],
        FileOpenerId::Webstorm => &["Contents/MacOS/webstorm"],
        FileOpenerId::FileManager => &[],
    }
}

#[cfg(windows)]
fn is_executable(path: &Path) -> bool {
    path.is_file()
}

fn editor_args(opener: FileOpenerId, target: &LocalFileTarget) -> Vec<OsString> {
    let mut args = opener
        .base_args()
        .iter()
        .map(OsString::from)
        .collect::<Vec<_>>();
    match opener.launch_style() {
        LaunchStyle::DirectPath => args.push(target.target_with_position()),
        LaunchStyle::Goto => {
            if target.line.is_some() {
                args.push(OsString::from("--goto"));
            }
            args.push(target.target_with_position());
        }
        LaunchStyle::LineColumn => {
            if let Some(line) = target.line {
                args.push(OsString::from("--line"));
                args.push(OsString::from(line.to_string()));
                if let Some(column) = target.column {
                    args.push(OsString::from("--column"));
                    args.push(OsString::from(column.to_string()));
                }
            }
            args.push(target.path.as_os_str().to_os_string());
        }
    }
    args
}

#[cfg(target_os = "macos")]
fn reveal_in_file_manager(path: &Path) -> Result<()> {
    if path.is_dir() {
        spawn_executable(
            Path::new("/usr/bin/open"),
            &[path.as_os_str().to_os_string()],
            "open Finder",
        )
    } else {
        spawn_executable(
            Path::new("/usr/bin/open"),
            &[OsString::from("-R"), path.as_os_str().to_os_string()],
            "reveal file in Finder",
        )
    }
}

#[cfg(all(unix, not(target_os = "macos")))]
fn reveal_in_file_manager(path: &Path) -> Result<()> {
    let target = if path.is_dir() {
        path
    } else {
        path.parent().unwrap_or_else(|| Path::new("."))
    };
    spawn_executable(
        Path::new("xdg-open"),
        &[target.as_os_str().to_os_string()],
        "open Files",
    )
}

#[cfg(windows)]
fn reveal_in_file_manager(path: &Path) -> Result<()> {
    if path.is_dir() {
        spawn_executable(
            Path::new("explorer"),
            &[path.as_os_str().to_os_string()],
            "open Explorer",
        )
    } else {
        let argument = format!("/select,\"{}\"", path.display());
        spawn_executable(
            Path::new("explorer"),
            &[OsString::from(argument)],
            "reveal file in Explorer",
        )
    }
}

fn spawn_executable(executable: &Path, args: &[OsString], action: &str) -> Result<()> {
    if executable.as_os_str().is_empty() {
        bail!("failed to {action}: executable is empty");
    }

    let mut command = command_for_executable(executable, args);
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .with_context(|| format!("failed to {action} with `{}`", executable.display()))?;
    Ok(())
}

#[cfg(not(windows))]
fn command_for_executable(executable: &Path, args: &[OsString]) -> Command {
    let mut command = Command::new(executable);
    command.args(args);
    command
}

#[cfg(windows)]
fn command_for_executable(executable: &Path, args: &[OsString]) -> Command {
    let extension = executable
        .extension()
        .and_then(OsStr::to_str)
        .unwrap_or_default();
    if extension.eq_ignore_ascii_case("cmd") || extension.eq_ignore_ascii_case("bat") {
        let mut command = Command::new("cmd.exe");
        command.arg("/C").arg(executable).args(args);
        command
    } else {
        let mut command = Command::new(executable);
        command.args(args);
        command
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn candidate_catalog_matches_t3code_order() {
        assert_eq!(FILE_OPENER_CANDIDATES.len(), 21);
        assert_eq!(FILE_OPENER_CANDIDATES[0], FileOpenerId::Cursor);
        assert_eq!(FILE_OPENER_CANDIDATES[6], FileOpenerId::Zed);
        assert_eq!(FILE_OPENER_CANDIDATES[20], FileOpenerId::FileManager);
        assert_eq!(FileOpenerId::Kiro.base_args(), &["ide"]);
        assert_eq!(FileOpenerId::Zed.commands(), &["zed", "zeditor"]);
    }

    #[test]
    fn editor_arguments_follow_t3code_launch_styles() {
        let target = LocalFileTarget {
            path: PathBuf::from("/tmp/example.rs"),
            line: Some(12),
            column: Some(4),
        };
        assert_eq!(
            editor_args(FileOpenerId::VisualStudioCode, &target),
            ["--goto", "/tmp/example.rs:12:4"]
                .into_iter()
                .map(OsString::from)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            editor_args(FileOpenerId::Pycharm, &target),
            ["--line", "12", "--column", "4", "/tmp/example.rs"]
                .into_iter()
                .map(OsString::from)
                .collect::<Vec<_>>()
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_discovery_rejects_stale_launchers_and_uses_bundled_cli() {
        use std::{fs, os::unix::fs::PermissionsExt as _};

        let temp = tempfile::tempdir().expect("temp dir");
        let missing_target = temp.path().join("Missing.app/Contents/MacOS/missing");
        let launcher = temp.path().join("missing-editor");
        fs::write(
            launcher.as_path(),
            format!(
                "#!/bin/bash\nopen -na \"{}\" --args \"$@\"\n",
                missing_target.display()
            ),
        )
        .expect("write launcher");
        fs::set_permissions(launcher.as_path(), fs::Permissions::from_mode(0o755))
            .expect("make launcher executable");
        assert!(!is_executable(launcher.as_path()));

        let zed_cli = temp.path().join("Zed.app/Contents/MacOS/cli");
        fs::create_dir_all(zed_cli.parent().expect("CLI parent")).expect("create app bundle");
        fs::write(zed_cli.as_path(), b"native-cli").expect("write bundled CLI");
        fs::set_permissions(zed_cli.as_path(), fs::Permissions::from_mode(0o755))
            .expect("make bundled CLI executable");
        assert_eq!(
            resolve_macos_app_executable_in_roots(FileOpenerId::Zed, &[temp.path().to_path_buf()]),
            Some(zed_cli)
        );
    }
}
