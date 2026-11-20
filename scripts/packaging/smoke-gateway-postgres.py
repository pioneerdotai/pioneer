#!/usr/bin/env python3
"""Exercise an installed Gateway and its child PostgreSQL."""

import argparse
import csv
import ctypes
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import tomllib
from urllib.error import URLError
from urllib.request import urlopen
import uuid

from websockets.sync.client import connect


def cli(binary, env, *args):
    return subprocess.check_output([binary, *args], env=env, text=True, timeout=90)


def status(binary, env):
    return json.loads(cli(binary, env, "status", "--json"))


def local_address(state):
    return state["listen_addr"].replace("0.0.0.0:", "127.0.0.1:", 1)


def accepts_sessions(state):
    if not state["service_active"] or not state["gateway_reachable"]:
        return False
    try:
        with urlopen(f"http://{local_address(state)}/ready", timeout=2) as response:
            return json.load(response)["accepting_new_turns"]
    except (URLError, TimeoutError):
        return False


def ready(binary, env):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        state = status(binary, env)
        if accepts_sessions(state):
            return state
        time.sleep(0.25)
    raise RuntimeError("Installed Gateway did not become ready")


def process_info(pid):
    if sys.platform == "darwin":
        library = ctypes.CDLL("libproc.dylib")
        buffer = ctypes.create_string_buffer(4096)
        if library.proc_pidpath(pid, buffer, len(buffer)) <= 0:
            return None
        parent = subprocess.run(
            ["/bin/ps", "-p", str(pid), "-o", "ppid="], text=True, timeout=10,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, check=False,
        )
        if parent.returncode != 0:
            return None
        return Path(os.fsdecode(buffer.value)), int(parent.stdout.strip())
    if sys.platform == "win32":
        row = subprocess.check_output([
            "pwsh", "-NoProfile", "-NonInteractive", "-Command",
            f"Get-CimInstance Win32_Process -Filter 'ProcessId={int(pid)}' | "
            "Select-Object ExecutablePath,ParentProcessId | ConvertTo-Json -Compress",
        ], text=True, timeout=10).strip()
        if not row:
            return None
        process = json.loads(row)
        if not process["ExecutablePath"]:
            return None
        return Path(process["ExecutablePath"]), process["ParentProcessId"]
    process = Path("/proc") / str(pid)
    try:
        executable = (process / "exe").resolve(strict=True)
        parent = int(next(line.split()[1] for line in (process / "status").read_text().splitlines()
                          if line.startswith("PPid:")))
    except FileNotFoundError:
        return None
    return executable, parent


def bundled_runtime(executable):
    if executable.parts[-5:] == ("cua", "Pioneer Gateway.app", "Contents", "MacOS", "pioneer"):
        return executable.parents[3] / "postgres"
    return executable.parent / "cua/postgres"


def program(runtime, name):
    return runtime / "bin" / (name + (".exe" if sys.platform == "win32" else ""))


def postmaster(state, gateway, runtime):
    home = Path(state["runtime_home"]).resolve()
    pid_file = home / "postgres/data/postmaster.pid"
    lines = pid_file.read_text().splitlines()
    pid = int(lines[0])
    process = process_info(pid)
    if process is None:
        raise RuntimeError("PostgreSQL pidfile does not identify a running process")
    executable, parent = process
    if executable.resolve(strict=True) != program(runtime, "postgres").resolve(strict=True):
        raise RuntimeError("Gateway did not start its bundled postgres")
    parent_info = process_info(parent)
    if parent_info is None or parent_info[0].resolve(strict=True) != gateway.resolve(strict=True):
        raise RuntimeError("Bundled postgres is not Gateway's direct child")
    if Path(lines[1]).resolve() != pid_file.parent:
        raise RuntimeError("Bundled postgres uses another data directory")
    if home.is_relative_to(runtime.parent.parent.parent):
        raise RuntimeError("Gateway stores PostgreSQL data inside the installation")
    if int(lines[3]) == 5432:
        raise RuntimeError("Bundled PostgreSQL bound the default system port")
    print(f"Bundled postgres {executable} (PID {pid}) is a child of Gateway PID {parent}",
          flush=True)
    return pid, pid_file, int(lines[3])


def rpc(state, secret, method, params):
    address = local_address(state)
    headers = {"Authorization": "Bearer " + secret, "Pioneer-Protocol-Version": "1"}
    request_id = uuid.uuid4().hex[:21]
    request = {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}
    with connect("ws://" + address + "/", additional_headers=headers, open_timeout=10) as ws:
        ws.send(json.dumps(request))
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            response = json.loads(ws.recv(timeout=max(0, deadline - time.monotonic())))
            if response.get("id") == request_id:
                if "error" in response:
                    raise RuntimeError(f"Gateway rejected {method}")
                return response["result"]
    raise RuntimeError(f"Gateway did not answer {method}")


def access_token(binary, env, state):
    pending = json.loads(cli(binary, env, "device", "create", "--json"))
    code = pending["activation_code"].replace("-", "")
    grant = rpc(state, code, "auth/device/activate", {"installation": {
        "installation_id": uuid.uuid4().hex,
        "display_name": "Gateway package smoke",
        "client_kind": "other",
    }})
    return grant["access_token"]


def verify_vector(runtime, pid_file, port):
    # Use the client shipped in the same archive. Feed the private credential
    # to the child's environment so it never enters argv or runner output.
    env = {key: value for key, value in os.environ.items() if not key.startswith("PG")}
    env.update({
        "PGPASSWORD": (pid_file.parents[1] / "password").read_text(),
        "PGSSLMODE": "disable",
        "LD_LIBRARY_PATH": str(runtime / "lib"),
    })
    query = (
        "SELECT current_setting('server_version_num')::int / 10000, "
        "'[1,2]'::vector <-> '[1,2]'::vector, extversion "
        "FROM pg_extension WHERE extname='vector'"
    )
    result = subprocess.check_output(
        [program(runtime, "psql"), "-XAt", "-w", "-v", "ON_ERROR_STOP=1", "-h", "127.0.0.1",
         "-p", str(port), "-U", "pioneer", "-d", "pioneer", "-c", query],
        env=env, text=True, timeout=15,
    )
    if result.strip() != "18|0|0.8.7":
        raise RuntimeError("Packaged PostgreSQL 18 with pgvector did not answer the distance query")
    print("Bundled PostgreSQL 18 with pgvector 0.8.7 returned the expected distance", flush=True)


def stop(binary, env, pid, pid_file):
    cli(binary, env, "stop")
    deadline = time.monotonic() + 30
    running = process_info(pid)
    while time.monotonic() < deadline and running:
        time.sleep(0.15)
        running = process_info(pid)
    if running or pid_file.exists():
        raise RuntimeError("Gateway stop left its postgres running")
    print(f"Postgres PID {pid} exited on Gateway stop", flush=True)


def click_start_local_gateway(env):
    capture = subprocess.check_output(
        ["import", "-window", "root", "-resize", "200%", "png:-"], env=env, timeout=10,
    )
    text = subprocess.check_output(
        ["tesseract", "stdin", "stdout", "-l", "eng", "--psm", "11", "tsv"],
        input=capture, env=env, timeout=10,
    ).decode()
    words = [
        row for row in csv.DictReader(text.splitlines(), delimiter="\t")
        if row.get("level") == "5" and row.get("text", "").strip()
    ]
    for start in range(len(words) - 2):
        label = words[start:start + 3]
        if [row["text"].lower() for row in label] != ["start", "local", "gateway"]:
            continue
        if len({(row["block_num"], row["par_num"], row["line_num"]) for row in label}) != 1:
            continue
        left = int(label[0]["left"])
        right = int(label[-1]["left"]) + int(label[-1]["width"])
        top = int(label[1]["top"])
        height = int(label[1]["height"])
        subprocess.run(
            ["xdotool", "mousemove", str((left + right) // 4), str((2 * top + height) // 4),
             "click", "1"],
            env=env, check=True, timeout=10,
        )
        return True
    return False


def start_from_desktop(binary, desktop, env):
    desktop_env = {**env, "APPIMAGE_EXTRACT_AND_RUN": "1", "LANG": "C.UTF-8",
                   "LANGUAGE": "en", "XDG_SESSION_TYPE": "x11",
                   "LIBGL_ALWAYS_SOFTWARE": "1"}
    desktop_env.pop("WAYLAND_DISPLAY", None)
    with tempfile.TemporaryFile() as log:
        process = subprocess.Popen([desktop], env=desktop_env, start_new_session=True,
                                   stdout=log, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 120
            next_click = 0
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RuntimeError("Packaged Desktop exited before starting its local Gateway")
                if binary.is_file():
                    state = status(binary, env)
                    install = state.get("install_state") or {}
                    registry_path = Path(state["runtime_home"]) / "gateway-registry.toml"
                    registry = tomllib.loads(registry_path.read_text()) if registry_path.is_file() else {}
                    if (install.get("managed_by") == "desktop"
                            and (registry.get("local") or {}).get("session_ref")
                            and accepts_sessions(state)):
                        return state
                if time.monotonic() >= next_click and click_start_local_gateway(desktop_env):
                    # The first run installs the embedded archive and pairs a device.
                    next_click = time.monotonic() + 15
                time.sleep(1)
            raise RuntimeError("Packaged Desktop did not start its embedded Gateway")
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                process.wait(timeout=15)


def smoke(binary, desktop):
    env = os.environ.copy()
    env.pop("PIONEER_TEST_POSTGRES_ADMIN_URL", None)
    workspace = "W" + uuid.uuid4().hex[:20]
    name = "Committed package smoke"
    try:
        state = start_from_desktop(binary, desktop, env) if desktop else ready(binary, env)
        executable = binary.resolve(strict=True)
        runtime = bundled_runtime(executable)
        pid, pid_file, port = postmaster(state, executable, runtime)
        token = access_token(binary, env, state)
        created = rpc(state, token, "workspace/create", {"workspace_id": workspace, "name": name})
        if created["workspace"]["id"] != workspace:
            raise RuntimeError("Gateway did not commit the requested workspace")
        verify_vector(runtime, pid_file, port)
        stop(binary, env, pid, pid_file)
        if not desktop:
            cli(binary, env, "start")
        restarted = start_from_desktop(binary, desktop, env) if desktop else ready(binary, env)
        new_pid, new_pid_file, _ = postmaster(restarted, executable, runtime)
        workspaces = rpc(restarted, token, "workspace/list", {})["workspaces"]
        if not any(row["id"] == workspace and row["name"] == name for row in workspaces):
            raise RuntimeError("Gateway lost the committed workspace after restart")
        print("Gateway read the committed workspace after restart", flush=True)
        stop(binary, env, new_pid, new_pid_file)
    finally:
        if binary.is_file() and status(binary, env)["service_active"]:
            cli(binary, env, "stop")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--desktop", type=Path)
    args = parser.parse_args()
    smoke(args.binary, args.desktop)
