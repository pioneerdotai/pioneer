#!/usr/bin/env python3
"""Guard only a symbols tool/build process tree; no elapsed-time cutoff."""
import os
import shutil
import signal
import subprocess
import sys
import time

reserve = 7 * 1024**3  # Stop before the user's 5 GiB floor, allowing pending IO.
if shutil.disk_usage(".").free < reserve:
    raise SystemExit("Insufficient free disk for a symbols task (7 GiB stop reserve)")
child = subprocess.Popen(sys.argv[1:], start_new_session=os.name != "nt")
last_report = 0
try:
    while child.poll() is None:
        free = shutil.disk_usage(".").free
        if free < reserve:
            print("Symbols task stopped: disk reserve reached", flush=True)
            raise RuntimeError("disk reserve")
        if time.monotonic() - last_report >= 60:
            print(f"Symbols worker pid={child.pid}; free={free / 1024**3:.2f} GiB", flush=True)
            last_report = time.monotonic()
        time.sleep(0.5)
except BaseException:
    if child.poll() is None:
        if os.name == "nt":
            subprocess.run(["taskkill", "/PID", str(child.pid), "/T", "/F"], check=False)
        else:
            try:
                os.killpg(child.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass  # Child exited between poll and signal; still reap below.
        try:
            child.wait(timeout=10)
        except subprocess.TimeoutExpired:
            if os.name != "nt":
                try:
                    os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            else:
                child.kill()
            child.wait()
    raise
raise SystemExit(child.wait())
