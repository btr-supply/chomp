#!/usr/bin/env python3
"""Chomp runtime status display."""

import os
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

try:
    from utils.runtime import runtime

    print("📊 Chomp Runtime Status")
    print("======================")

    config = runtime.get_config()
    instance_info = runtime.get_instance_info()
    pids = runtime.get_pids()

    print(f"Instance Name: {instance_info['name']}")
    print(f"Instance UID:  {instance_info['uid']}")
    print(f"Configuration: {config['MODE']} {config['DEPLOYMENT']} {config['API']}")

    # Check running processes
    api_pid = pids.get('api_pid')
    other_pids = pids.get('pids', [])

    if api_pid or other_pids:
        print("Running Processes:")
        if api_pid:
            try:
                os.kill(api_pid, 0)
                print(f"  API Server: PID {api_pid} (running)")
            except (OSError, ProcessLookupError):
                print(f"  API Server: PID {api_pid} (not running)")

        for pid in other_pids:
            try:
                os.kill(pid, 0)
                print(f"  Process: PID {pid} (running)")
            except (OSError, ProcessLookupError):
                print(f"  Process: PID {pid} (not running)")
    else:
        print("Running Processes: None tracked")

except Exception as e:
    print(f"Error reading runtime state: {e}")
    sys.exit(1)
