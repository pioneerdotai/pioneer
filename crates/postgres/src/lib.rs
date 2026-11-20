//! Lifecycle of the private PostgreSQL server delivered with Gateway.

use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    net::{Ipv4Addr, TcpListener},
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, ensure};
use fs4::FileExt;
use migration::{Migrator, MigratorTrait};
use process_wrap::tokio::{ChildWrapper, CommandWrap, KillOnDrop};
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseConnection, DbBackend, Statement,
};
use tokio::{process::Command, time::Instant};
use url::Url;
use uuid::Uuid;

const ROLE: &str = "pioneer";
const READER_ROLE: &str = "pioneer_reader";
const DATABASE: &str = "pioneer";
const START_TIMEOUT: Duration = Duration::from_secs(40);
const STOP_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, thiserror::Error)]
#[error("PostgreSQL data already owned by a Gateway")]
pub struct ClusterBusy;

pub fn data_directory(home: &Path) -> PathBuf {
    home.join("postgres/data")
}

struct Runtime {
    root: PathBuf,
}

impl Runtime {
    fn locate(executable: &Path) -> Result<Self> {
        let parent = executable
            .parent()
            .context("Gateway executable has no directory")?;
        let companions = if cfg!(target_os = "macos")
            && executable.ends_with("cua/Pioneer Gateway.app/Contents/MacOS/pioneer")
        {
            // Both the signed Desktop archive and the standalone installer
            // lay out macOS CU with the same companion directory.
            executable
                .ancestors()
                .nth(4)
                .context("invalid Gateway bundle path")?
                .to_path_buf()
        } else {
            parent.join("cua")
        };
        let root = companions.join("postgres");
        for program in ["postgres", "initdb", "pg_ctl"] {
            ensure!(
                program_path(&root, program).is_file(),
                "missing bundled PostgreSQL program {program}"
            );
        }
        Ok(Self { root })
    }

    fn command(&self, program: &str) -> Command {
        let mut command = Command::new(program_path(&self.root, program));
        command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        // Never let ambient Postgres client settings redirect bundled tools.
        for (key, _) in std::env::vars_os() {
            if key.as_encoded_bytes().starts_with(b"PG") {
                command.env_remove(key);
            }
        }
        #[cfg(target_os = "linux")]
        command.env("LD_LIBRARY_PATH", self.root.join("lib"));
        command
    }
}

fn program_path(root: &Path, name: &str) -> PathBuf {
    root.join("bin")
        .join(format!("{name}{}", std::env::consts::EXE_SUFFIX))
}

struct Cluster {
    directory: PathBuf,
    password: String,
    reader_password: String,
    _owner: File,
}

impl Cluster {
    async fn open(home: &Path, runtime: &Runtime, initialize: bool) -> Result<Self> {
        let root = home.join("postgres");
        let mut builder = fs::DirBuilder::new();
        builder.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder
            .create(&root)
            .context("create PostgreSQL runtime directory")?;
        let owner = private_lock_file(&root.join("owner.lock"))
            .context("open PostgreSQL ownership lock")?;
        match FileExt::try_lock(&owner) {
            Ok(()) => {}
            Err(fs4::TryLockError::WouldBlock) => return Err(ClusterBusy.into()),
            Err(error) => return Err(error).context("lock private PostgreSQL data"),
        }
        let directory = data_directory(home);
        if !initialize {
            ensure!(directory.is_dir(), "private PostgreSQL data is missing");
        }
        let password_file = root.join("password");
        let password = read_or_create_password(&password_file, !directory.exists())?;
        let reader_password = read_or_create_password(&root.join("reader-password"), initialize)?;

        if !directory.exists() {
            let staged = tempfile::Builder::new()
                .prefix(".init-")
                .tempdir_in(&root)?;
            let status = runtime
                .command("initdb")
                .arg("-D")
                .arg(staged.path())
                .arg("--username")
                .arg(ROLE)
                .arg("--auth=scram-sha-256")
                .arg("--locale-provider=builtin")
                .arg("--builtin-locale=C.UTF-8")
                .arg("--encoding=UTF8")
                .arg("--pwfile")
                .arg(&password_file)
                .kill_on_drop(true)
                .status()
                .await
                .context("launch bundled initdb")?;
            ensure!(status.success(), "bundled initdb failed ({status})");
            // Publish a fully initialized cluster only. An interrupted initdb
            // leaves the existing data and the credential available on retry.
            fs::rename(staged.path(), &directory)
                .context("publish initialized PostgreSQL cluster")?;
        }
        let version = fs::read_to_string(directory.join("PG_VERSION"))
            .context("read PostgreSQL data version")?;
        ensure!(
            version.trim() == "18",
            "private PostgreSQL data needs a major version upgrade"
        );
        Ok(Self {
            directory,
            password,
            reader_password,
            _owner: owner,
        })
    }

    fn options(&self, port: u16, database: &str, max_connections: u32) -> ConnectOptions {
        connect_options(ROLE, &self.password, port, database, max_connections)
    }
}

fn read_or_create_password(path: &Path, may_create: bool) -> Result<String> {
    match fs::read_to_string(path) {
        Ok(saved) => {
            ensure!(
                saved.len() == 64 && saved.bytes().all(|b| b.is_ascii_hexdigit()),
                "invalid private PostgreSQL credential"
            );
            Ok(saved)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && may_create => {
            let value = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
            let mut file = tempfile::NamedTempFile::new_in(
                path.parent()
                    .context("PostgreSQL credential directory missing")?,
            )?;
            file.write_all(value.as_bytes())?;
            file.as_file().sync_all()?;
            file.persist_noclobber(path)?;
            Ok(value)
        }
        Err(error) => Err(error).context("read private PostgreSQL credential"),
    }
}

fn connect_options(
    role: &str,
    password: &str,
    port: u16,
    database: &str,
    max_connections: u32,
) -> ConnectOptions {
    let mut url = Url::parse("postgresql://127.0.0.1").expect("constant PostgreSQL URL");
    url.set_username(role).expect("constant PostgreSQL role");
    url.set_port(Some(port)).expect("constant PostgreSQL host");
    url.set_path(database);
    url.set_password(Some(password))
        .expect("constant PostgreSQL user");
    url.query_pairs_mut().append_pair("sslmode", "disable");
    let mut options = ConnectOptions::new(url.to_string());
    options
        .min_connections(0)
        .max_connections(max_connections)
        .connect_timeout(Duration::from_secs(2))
        .acquire_timeout(Duration::from_secs(3))
        .sqlx_logging(false);
    options
}

fn read_only_options(mut options: ConnectOptions) -> ConnectOptions {
    options.map_sqlx_postgres_opts(|options| {
        options.options([("default_transaction_read_only", "on")])
    });
    options
}

fn private_lock_file(path: &Path) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

pub struct BundledPostgres {
    runtime: Runtime,
    cluster: Cluster,
    child: Box<dyn ChildWrapper>,
    port: u16,
}

impl BundledPostgres {
    pub async fn start(home: &Path) -> Result<Self> {
        let executable = std::env::current_exe().context("locate Gateway executable")?;
        let runtime = Runtime::locate(&executable)?;
        let cluster = Cluster::open(home, &runtime, true).await?;
        recover_postmaster(&runtime, &cluster).await?;
        let instance = Self::launch(runtime, cluster).await?;
        let result = instance.prepare().await;
        if let Err(error) = result {
            let _ = instance.stop().await;
            return Err(error);
        }
        Ok(instance)
    }

    /// Open an existing cluster with its installed server without applying migrations.
    pub async fn start_existing(home: &Path, executable: &Path) -> Result<Self> {
        let runtime = Runtime::locate(&fs::canonicalize(executable)?)?;
        let cluster = Cluster::open(home, &runtime, false).await?;
        recover_postmaster(&runtime, &cluster).await?;
        Self::launch(runtime, cluster).await
    }

    /// Connect to the current owner for read-only inspection. Authenticate the
    /// cluster identity before accepting a port from its postmaster file.
    pub async fn running_reader(home: &Path) -> Result<DatabaseConnection> {
        let directory = data_directory(home);
        let pid_file = fs::read_to_string(directory.join("postmaster.pid"))
            .context("read private PostgreSQL postmaster status")?;
        let port = postmaster_port(&pid_file).context("invalid private PostgreSQL port")?;
        let password = fs::read_to_string(home.join("postgres/password"))
            .context("read private PostgreSQL credential")?;
        let options = connect_options(ROLE, &password, port, DATABASE, 1);
        let verification = Database::connect(options.clone())
            .await
            .map_err(|_| anyhow!("connect to private PostgreSQL for cluster verification"))?;
        let verified = verify_cluster(&verification, &directory).await;
        let closed = verification.close().await;
        verified?;
        closed.context("close PostgreSQL cluster verification pool")?;
        let reader_password =
            read_or_create_password(&home.join("postgres/reader-password"), false)?;
        Database::connect(read_only_options(connect_options(
            READER_ROLE,
            &reader_password,
            port,
            DATABASE,
            1,
        )))
        .await
        .map_err(|_| anyhow!("connect to private PostgreSQL reader"))
    }

    async fn launch(runtime: Runtime, cluster: Cluster) -> Result<Self> {
        for _ in 0..4 {
            // Port 5432 is irrelevant to this instance. Retry the small race
            // between releasing the loopback reservation and PostgreSQL bind.
            let socket = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
                .context("reserve private PostgreSQL port")?;
            let port = socket.local_addr()?.port();
            drop(socket);
            let mut command = runtime.command("postgres");
            command
                .arg("-D")
                .arg(&cluster.directory)
                .arg("-h")
                .arg("127.0.0.1")
                .arg("-p")
                .arg(port.to_string())
                .arg("-c")
                .arg("unix_socket_directories=")
                .arg("-c")
                .arg("logging_collector=off");
            #[cfg(target_os = "linux")]
            {
                let parent = std::process::id();
                // SIGINT asks postgres for a fast, recoverable shutdown.
                unsafe {
                    command.pre_exec(move || {
                        if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGINT) == -1 {
                            return Err(std::io::Error::last_os_error());
                        }
                        if libc::getppid() as u32 != parent {
                            return Err(std::io::Error::other("Gateway exited before PostgreSQL"));
                        }
                        Ok(())
                    });
                }
            }
            let mut wrapped = CommandWrap::from(command);
            wrapped.wrap(KillOnDrop);
            #[cfg(windows)]
            wrapped.wrap(process_wrap::tokio::JobObject);
            let mut child = wrapped.spawn().context("launch bundled postgres")?;
            let deadline = Instant::now() + START_TIMEOUT;
            loop {
                if child.try_wait()?.is_some() {
                    break;
                }
                if let Ok(connection) =
                    Database::connect(cluster.options(port, "postgres", 1)).await
                {
                    let matches = verify_cluster(&connection, &cluster.directory).await;
                    connection
                        .close()
                        .await
                        .map_err(|_| anyhow!("close PostgreSQL readiness connection"))?;
                    matches?;
                    return Ok(Self {
                        runtime,
                        cluster,
                        child,
                        port,
                    });
                }
                ensure!(
                    Instant::now() < deadline,
                    "bundled postgres did not become ready"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        anyhow::bail!("bundled postgres exited before readiness")
    }

    async fn prepare(&self) -> Result<()> {
        let admin = self.connect_to("postgres", 1).await?;
        let result = async {
            let exists = admin
                .query_one_raw(Statement::from_sql_and_values(
                    DbBackend::Postgres,
                    "SELECT 1 FROM pg_database WHERE datname = $1",
                    [DATABASE.into()],
                ))
                .await?
                .is_some();
            if !exists {
                // PostgreSQL CREATE DATABASE cannot bind identifiers. This is
                // the fixed application database owned by this private role.
                admin.execute_unprepared("CREATE DATABASE pioneer").await?;
            }
            let reader_exists = admin
                .query_one_raw(Statement::from_string(
                    DbBackend::Postgres,
                    "SELECT 1 FROM pg_roles WHERE rolname = 'pioneer_reader'",
                ))
                .await?
                .is_some();
            let operation = if reader_exists { "ALTER" } else { "CREATE" };
            // PostgreSQL role utilities do not bind the password argument.
            // The private credential is validated hexadecimal and this error
            // intentionally omits server diagnostics that might echo the SQL.
            let role_sql = format!(
                "{operation} ROLE {READER_ROLE} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD '{}'",
                self.cluster.reader_password,
            );
            admin.execute_unprepared(&role_sql)
                .await
                .map_err(|_| anyhow!("configure the private PostgreSQL reader role"))?;
            admin.execute_unprepared("GRANT pg_read_all_data TO pioneer_reader").await?;
            Ok::<_, anyhow::Error>(())
        }
        .await;
        let admin_closed = admin.close().await;
        result.context("prepare private Gateway database")?;
        admin_closed.context("close private PostgreSQL administrative pool")?;
        let database = self.connect_to(DATABASE, 1).await?;
        let migrated = Migrator::up(&database, None).await;
        let closed = database.close().await;
        migrated.context("apply Gateway PostgreSQL migrations")?;
        closed.context("close Gateway migration pool")?;
        Ok(())
    }

    async fn connect_to(&self, database: &str, max_connections: u32) -> Result<DatabaseConnection> {
        Database::connect(self.cluster.options(self.port, database, max_connections))
            .await
            .map_err(|_| anyhow!("connect to private Gateway PostgreSQL"))
    }

    pub fn connect_options(&self, max_connections: u32) -> ConnectOptions {
        self.cluster.options(self.port, DATABASE, max_connections)
    }

    pub fn reader_options(&self, max_connections: u32) -> ConnectOptions {
        read_only_options(connect_options(
            READER_ROLE,
            &self.cluster.reader_password,
            self.port,
            DATABASE,
            max_connections,
        ))
    }

    /// Gateway monitors the child until it begins an orderly shutdown.
    pub async fn wait_for_unexpected_exit(&mut self) -> Result<()> {
        let status = self
            .child
            .wait()
            .await
            .context("monitor bundled PostgreSQL")?;
        anyhow::bail!("bundled PostgreSQL exited while Gateway was running ({status})");
    }

    /// Close Gateway pools before asking PostgreSQL to flush and exit.
    pub async fn stop(mut self) -> Result<()> {
        if self.child.try_wait()?.is_none() {
            stop_cluster(&self.runtime, &self.cluster.directory).await?;
        }
        let status = tokio::time::timeout(STOP_TIMEOUT, self.child.wait())
            .await
            .context("wait for bundled PostgreSQL shutdown")??;
        ensure!(
            status.success(),
            "bundled PostgreSQL shutdown failed ({status})"
        );
        Ok(())
    }
}

async fn verify_cluster(connection: &DatabaseConnection, expected: &Path) -> Result<()> {
    let row = connection
        .query_one_raw(Statement::from_string(
            DbBackend::Postgres,
            "SELECT current_setting('data_directory') AS directory",
        ))
        .await?
        .context("private PostgreSQL data directory unavailable")?;
    let directory: String = row.try_get("", "directory")?;
    ensure!(
        fs::canonicalize(directory)? == fs::canonicalize(expected)?,
        "PostgreSQL response came from another cluster"
    );
    Ok(())
}

async fn stop_cluster(runtime: &Runtime, directory: &Path) -> Result<()> {
    let status = tokio::time::timeout(
        STOP_TIMEOUT,
        runtime
            .command("pg_ctl")
            .arg("stop")
            .arg("-D")
            .arg(directory)
            .args(["-m", "fast", "-w", "-t", "25"])
            .kill_on_drop(true)
            .status(),
    )
    .await
    .context("bundled pg_ctl did not finish")??;
    ensure!(
        status.success(),
        "bundled PostgreSQL refused shutdown ({status})"
    );
    Ok(())
}

async fn recover_postmaster(runtime: &Runtime, cluster: &Cluster) -> Result<()> {
    let pid_file = match fs::read_to_string(cluster.directory.join("postmaster.pid")) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error).context("read private postmaster status"),
    };
    // PostgreSQL handles stale PID files itself. Only request shutdown after
    // authenticating and checking the live listener's data directory.
    let Some(port) = postmaster_port(&pid_file) else {
        return Ok(());
    };
    let Ok(connection) = Database::connect(cluster.options(port, "postgres", 1)).await else {
        return Ok(());
    };
    let matches = verify_cluster(&connection, &cluster.directory).await;
    connection
        .close()
        .await
        .map_err(|_| anyhow!("close recovered PostgreSQL connection"))?;
    matches?;
    stop_cluster(runtime, &cluster.directory).await
}

fn postmaster_port(pid_file: &str) -> Option<u16> {
    pid_file
        .lines()
        .nth(3)?
        .parse()
        .ok()
        .filter(|port| *port != 0)
}
