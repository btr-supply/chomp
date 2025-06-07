"""
Entry point for running the Chomp server as a module.
This allows running the server with: python -m chomp.src.server
"""

import sys
import asyncio
from pathlib import Path

# Add the project root to Python path so we can import from chomp
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from chomp.src.utils import ArgParser  # noqa: E402
from chomp.src import state  # noqa: E402
from chomp.src.utils import log_info  # noqa: E402
from chomp.src.server import start  # noqa: E402


async def main():
  """Main entry point for the server with proper argument initialization."""

  # Initialize argument parser with server-specific defaults
  ap = ArgParser(
      description="Chomp server for data retrieval and real-time forwarding.")
  ap.add_groups({
      "Common runtime": [
          (("-e", "--env"), str, ".env", None, "Environment file if any"),
          (
              ("-v", "--verbose"),
              bool,
              False,
              "store_true",
              "Verbose output (loglevel debug)",
          ),
          (
              ("-i", "--proc_id"),
              str,
              "chomp-server",
              None,
              "Unique instance identifier",
          ),
          (
              ("-t", "--threaded"),
              bool,
              True,
              "store_true",
              "Run jobs/routers in separate threads",
          ),
          (
              ("-a", "--tsdb_adapter"),
              str,
              "tdengine",
              None,
              "Timeseries database adapter",
          ),
      ],
      "Server runtime": [
          (
              ("-s", "--server"),
              bool,
              True,
              "store_true",
              "Run as server (always true for this entry point)",
          ),
          (("-sh", "--host"), str, "127.0.0.1", None, "FastAPI server host"),
          (("-sp", "--port"), int, 40004, None, "FastAPI server port"),
          (
              ("-wpi", "--ws_ping_interval"),
              int,
              30,
              None,
              "Websocket server ping interval",
          ),
          (
              ("-wpt", "--ws_ping_timeout"),
              int,
              20,
              None,
              "Websocket server ping timeout",
          ),
          (
              ("-pi", "--ping"),
              bool,
              False,
              "store_true",
              "Ping DB and cache for readiness",
          ),
      ],
  })

  # Initialize state with parsed arguments
  state.init(args_=ap.load_env())
  log_info(f"Server starting with arguments\n{ap.pretty()}")

  # Start the server
  await start()


if __name__ == "__main__":
  asyncio.run(main())
