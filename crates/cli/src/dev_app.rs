//! Local App authoring commands. The Gateway owns registration and observation.
use anyhow::{Context, Result, bail, ensure};
use pioneer_protocol::{AppsDetailsParams, AppsDevAddParams, AppsListParams, DevAppOperation};
use std::path::PathBuf;

struct DevAppArgs {
    command: String,
    project: Option<PathBuf>,
    id: Option<String>,
    url: Option<String>,
    workspace: Option<String>,
    gateway: Option<String>,
}
fn parse(mut args: impl Iterator<Item = String>) -> Result<DevAppArgs> {
    ensure!(
        args.next().as_deref() == Some("app"),
        "expected: pioneer dev app add <project> --url <origin> | list | remove <id>"
    );
    let command = args.next().context("missing dev app command")?;
    let mut project = None;
    let mut id = None;
    let mut url = None;
    let mut workspace = None;
    let mut gateway = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--url" => {
                ensure!(url.is_none(), "duplicate --url");
                url = Some(args.next().context("--url requires an origin")?);
            }
            "--workspace" => {
                ensure!(workspace.is_none(), "duplicate --workspace");
                workspace = Some(args.next().context("--workspace requires an ID")?);
            }
            "--gateway" => {
                ensure!(gateway.is_none(), "duplicate --gateway");
                gateway = Some(args.next().context("--gateway requires a registry ID")?);
            }
            value if !value.starts_with('-') && command == "add" && project.is_none() => {
                project = Some(PathBuf::from(value))
            }
            value if !value.starts_with('-') && command == "remove" && id.is_none() => {
                id = Some(value.to_owned())
            }
            _ => bail!("unexpected dev app argument: {arg}"),
        }
    }
    match command.as_str() {
        "add" => {
            ensure!(project.is_some(), "add requires a project directory");
            ensure!(url.is_some(), "add requires --url http://localhost:PORT");
        }
        "list" => {
            ensure!(url.is_none(), "--url applies only to add");
        }
        "remove" => {
            ensure!(id.is_some(), "remove requires a dev connection ID");
            ensure!(url.is_none(), "--url applies only to add");
        }
        _ => bail!("unknown dev app command: {command}; expected add, list, remove"),
    }
    Ok(DevAppArgs {
        command,
        project,
        id,
        url,
        workspace,
        gateway,
    })
}
pub(crate) fn run(args: impl Iterator<Item = String>) -> Result<()> {
    let DevAppArgs {
        command,
        project,
        id,
        url,
        workspace,
        gateway,
    } = parse(args)?;
    let config = pioneer_config::AppConfig::load()?;
    let home = config.runtime_home_dir()?;
    let path = home.join(config.desktop.gateway.registry_file_name.trim());
    let registry: pioneer_client::gateway::types::GatewayRegistry =
        toml::from_str(&std::fs::read_to_string(path).context(
            "Gateway registry unavailable; select a local Gateway/workspace in Pioneer first",
        )?)?;
    let endpoint = if let Some(id) = &gateway {
        registry
            .local
            .iter()
            .chain(registry.remotes.iter())
            .find(|e| &e.id == id)
    } else {
        registry.active_gateway()
    }
    .context("selected Gateway is unavailable")?;
    ensure!(
        endpoint.kind == pioneer_client::gateway::types::GatewayEndpointKind::Local,
        "dev apps require sources and dev server on the local Gateway computer; remote Gateways and tunnels are unsupported"
    );
    let expected =
        pioneer_protocol::GatewayBaseUrl::from_local_listen_addr(&config.gateway.listen_addr)?;
    ensure!(
        endpoint.gateway_base_url == expected,
        "selected Gateway does not match this runtime's local Gateway; remote topology is unsupported"
    );
    let workspace = workspace
        .or_else(|| endpoint.workspace_id.clone())
        .context("select a workspace in Pioneer or pass --workspace <id>")?;
    let operation = match command.as_str() {
        "add" => DevAppOperation::Add(AppsDevAddParams {
            workspace_id: workspace,
            project: project
                .context("add requires a project directory")?
                .canonicalize()?
                .to_str()
                .context("project path must be UTF-8")?
                .to_owned(),
            url: url.context("add requires --url http://localhost:PORT")?,
        }),
        "list" => {
            ensure!(url.is_none(), "--url applies only to add");
            DevAppOperation::List(AppsListParams {
                workspace_id: workspace,
            })
        }
        "remove" => {
            ensure!(url.is_none(), "--url applies only to add");
            DevAppOperation::Remove(AppsDetailsParams {
                workspace_id: workspace,
                app_id: id.context("remove requires a dev connection ID")?,
            })
        }
        _ => bail!("unknown dev app command: {command}; expected add, list, remove"),
    };
    let result = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(pioneer_gateway::dev_app_operation(
            &config, &home, operation,
        ))?;
    match command.as_str() {
        "add" => println!("{}", serde_json::to_string_pretty(&result)?),
        "list" => {
            let connections: Vec<pioneer_protocol::AppDevConnection> =
                serde_json::from_value(result)?;
            for item in connections {
                println!(
                    "{}  {}  {}  {}  {}",
                    item.id, item.name, item.project, item.url, item.state
                );
                if let Some(diagnostic) = item.diagnostic {
                    println!("  diagnostic: {diagnostic}");
                }
            }
        }
        _ => println!("removed"),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dev_app_cli_requires_add_project_and_explicit_url() {
        let parsed = parse(
            [
                "app",
                "add",
                ".",
                "--url",
                "http://localhost:3000",
                "--workspace",
                "chosen",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap();
        assert_eq!(parsed.project, Some(PathBuf::from(".")));
        assert_eq!(parsed.url.as_deref(), Some("http://localhost:3000"));
        assert_eq!(parsed.workspace.as_deref(), Some("chosen"));
        for arguments in [
            vec!["app", "add", "--url", "http://localhost:3000"],
            vec!["app", "add", "."],
            vec!["app", "run", ".", "--url", "http://localhost:3000"],
            vec!["app", "list", "."],
            vec!["app", "remove"],
        ] {
            assert!(parse(arguments.into_iter().map(str::to_owned)).is_err());
        }
    }
}
