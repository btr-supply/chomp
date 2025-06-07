from asyncio import gather, run, sleep
from typing import Type
from pathlib import Path

from src.utils import (
    log_info,
    log_warn,
    log_error,
    log_debug,
    ArgParser,
    generate_hash,
    prettify,
)
from src import state
from src.model import Config, Tsdb, TsdbAdapter


def get_adapter_class(adapter: TsdbAdapter) -> Type[Tsdb] | None:
  """
    Dynamically load database adapter classes, only including those that can be imported.
    This allows the system to work with different database dependencies without breaking.
    """
  from src.utils import safe_import

  # Mapping of adapter names to their module paths and class names
  adapter_mapping = {
      "tdengine": ("src.adapters.tdengine", "Taos"),
      "sqlite": ("src.adapters.sqlite", "SQLite"),
      "clickhouse": ("src.adapters.clickhouse", "ClickHouse"),
      "duckdb": ("src.adapters.duckdb", "DuckDB"),
      "timescale": ("src.adapters.timescale", "TimescaleDb"),
      "opentsdb": ("src.adapters.opentsdb", "OpenTsdb"),
      "questdb": ("src.adapters.questdb", "QuestDb"),
      "mongodb": ("src.adapters.mongodb", "MongoDb"),
      "influxdb": ("src.adapters.influxdb", "InfluxDb"),
      "victoriametrics": ("src.adapters.victoriametrics", "VictoriaMetrics"),
      "kx": ("src.adapters.kx", "Kx"),
  }

  # Build implementations dict dynamically, only including available adapters
  implementations: dict[TsdbAdapter, Type[Tsdb]] = {}
  for name, (module_path, class_name) in adapter_mapping.items():
    adapter_class = safe_import(module_path, class_name)
    if adapter_class is not None:
      implementations[name] = adapter_class  # type: ignore[assignment]

  requested_adapter = adapter.lower()  # type: ignore[assignment]
  return implementations.get(requested_adapter)


def get_available_adapters() -> list[str]:
  """
    Get a list of all available database adapters that can be imported.
    Useful for debugging and providing helpful error messages.
    """
  from src.utils import safe_import

  adapter_mapping = {
      "tdengine": ("src.adapters.tdengine", "Taos"),
      "sqlite": ("src.adapters.sqlite", "SQLite"),
      "clickhouse": ("src.adapters.clickhouse", "ClickHouse"),
      "duckdb": ("src.adapters.duckdb", "DuckDB"),
      "timescale": ("src.adapters.timescale", "TimescaleDb"),
      "opentsdb": ("src.adapters.opentsdb", "OpenTsdb"),
      "questdb": ("src.adapters.questdb", "QuestDb"),
      "mongodb": ("src.adapters.mongodb", "MongoDb"),
      "influxdb": ("src.adapters.influxdb", "InfluxDb"),
      "victoriametrics": ("src.adapters.victoriametrics", "VictoriaMetrics"),
      "kx": ("src.adapters.kx", "Kx"),
  }

  available = []
  for name, (module_path, class_name) in adapter_mapping.items():
    if safe_import(module_path, class_name) is not None:
      available.append(name)

  return sorted(available)


def create_instance_monitor():
  """Create a system monitor ingester for collecting instance vitals every 30 seconds."""
  from src.model import Ingester, ResourceField
  from src import state

  # Use instance name to ensure unique table per instance
  instance_name = (state.instance.name if hasattr(state, "instance")
                   and state.instance else "chomp_instance")
  monitor_name = f"{instance_name}_monitor"

  return Ingester(
      name=monitor_name,
      resource_type="timeseries",
      ingester_type="processor",  # Use valid IngesterType
      interval="s30",  # 30 second interval
      fields=[
          ResourceField(name="ts", type="timestamp"),
          ResourceField(name="instance_name", type="string"),
          ResourceField(name="resources_count", type="int32"),
          ResourceField(name="cpu_usage", type="float64"),
          ResourceField(name="memory_usage", type="float64"),
          ResourceField(name="disk_usage", type="float64"),
          # Geolocation fields (transient - cached but not stored in time series)
          ResourceField(name="coordinates", type="string", transient=True),
          ResourceField(name="timezone", type="string", transient=True),
          ResourceField(name="country_code", type="string", transient=True),
          ResourceField(name="location", type="string", transient=True),
          ResourceField(name="isp", type="string", transient=True),
      ],
  )


async def start_ingester(config: Config):
  # ingester specific imports
  from src.cache import is_task_claimed, ping as redis_ping
  from src.actions import schedule, scheduler

  # Validate Redis connection before proceeding
  try:
    redis_connected = await redis_ping()
    if not redis_connected:
      log_error(
          "Failed to connect to Redis. Cannot proceed with task claiming.")
      return
    log_info("✅ Redis connection verified")
  except Exception as e:
    log_error(f"Redis connection error: {e}")
    return

  ingesters = config.ingesters

  # Validate configuration loading
  if not ingesters:
    log_error(
        "No ingesters found in configuration! Check your configuration files.")
    return

  log_info(f"Loaded {len(ingesters)} ingester configurations")
  if state.args.verbose:
    for ingester in ingesters:
      log_debug(
          f"  - {ingester.name} ({ingester.ingester_type}, {ingester.interval}, {len(ingester.fields)} fields)"
      )

  # Instance initialization is handled in state.init() during startup

  # Add system monitor ingester if monitored flag is set
  if state.args.monitored:
    monitor_ingester = create_instance_monitor()
    ingesters.append(monitor_ingester)
    log_info("✅ System monitor ingester added to schedule")

  # late import to avoid circular dependency
  from src.cache import ensure_claim_task

  # Retry mechanism: try to claim tasks with exponential backoff
  max_retries = 5
  retry_delay = 30  # Start with 30 seconds
  max_retry_delay = 300  # Max 5 minutes

  for retry_attempt in range(max_retries):
    unclaimed = [c for c in ingesters if not await is_task_claimed(c)]
    in_range = unclaimed[:state.args.max_jobs]

    if not unclaimed:
      log_warn("All tasks are currently claimed by other workers")

    if not in_range:
      log_warn(
          "No tasks available for this worker (max_jobs limit or all claimed)")

    table_data = []
    claims = 0
    for c in ingesters:
      if c in in_range:
        claims += 1
      claim_str = f"({claims}/{state.args.max_jobs})"
      table_data.append([
          c.name[:21].ljust(24, ".") if len(c.name) > 24 else c.name,
          c.ingester_type,
          c.interval,
          len(c.fields),
          "yes 🟢" if c in unclaimed else "no 🔴",
          f"yes 🟢 {claim_str}" if c in in_range else "no 🔴",
      ])

    log_info(
        f"\n{prettify(table_data, ['Resource', 'Ingester', 'Interval', 'Fields', 'Claimable', 'Picked-up'])}"
    )

    # claim tasks before scheduling - handle failures gracefully
    successfully_claimed = []
    for c in in_range:
      try:
        await ensure_claim_task(c)
        successfully_claimed.append(c)
        log_info(f"Successfully claimed task: {c.name}")
      except ValueError as e:
        log_warn(f"Failed to claim task {c.name}: {e}")
        continue
      except Exception as e:
        log_error(f"Unexpected error claiming task {c.name}: {e}")
        continue

    if successfully_claimed:
      log_info(
          f"Successfully claimed {len(successfully_claimed)} out of {len(in_range)} candidate tasks"
      )
      break  # Exit retry loop if we claimed any tasks

    # If no tasks claimed and this is not the last retry
    if retry_attempt < max_retries - 1:
      log_warn(
          f"No tasks could be claimed on attempt {retry_attempt + 1}/{max_retries}. Retrying in {retry_delay} seconds..."
      )
      await sleep(retry_delay)
      retry_delay = min(retry_delay * 1.5,
                        max_retry_delay)  # Exponential backoff with cap
    else:
      log_warn(
          "No tasks available after all retry attempts. This worker will exit as other workers have claimed all tasks."
      )
      return

  # schedule only successfully claimed tasks
  schedule_tasks = [schedule(c) for c in successfully_claimed]
  try:
    scheduled_results = await gather(*schedule_tasks)
    # Flatten results and filter out None tasks
    all_tasks = [
        task for result in scheduled_results for task in result
        if task is not None
    ]
    if all_tasks:
      await gather(*all_tasks)
    else:
      log_warn("No task scheduled")
  except Exception as e:
    log_error(f"Error during task scheduling: {e}")
    return

  cron_monitors = await scheduler.start(threaded=state.args.threaded)
  if not cron_monitors:
    log_warn(
        "No cron scheduled, tasks picked up by other workers. Shutting down..."
    )
    return
  await gather(*cron_monitors)  # type: ignore


async def start_server(config: Config):
  # server specific imports
  from src.server import start

  # Instance initialization is handled in state.init() during startup

  await start()


async def main(ap: ArgParser):
  # load environment variables and initialize db adapters
  await state.init(ap.load_env())
  log_info(f"Arguments\n{ap.pretty()}")

  # Only validate configuration files for ingesters, not for servers
  if not state.args.server:
    # Validate configuration files exist before proceeding
    config_files = [
        file_path.strip()
        for file_path in state.args.ingester_configs.split(",")
        if file_path.strip()
    ]
    missing_files = []
    for config_file in config_files:
      abs_path = Path(config_file).resolve()
      if not abs_path.exists():
        missing_files.append(f"{config_file} (resolved to: {abs_path})")

    if missing_files:
      log_error("Configuration file(s) not found:")
      for missing_file in missing_files:
        log_error(f"  - {missing_file}")
      log_error(f"Working directory: {Path.cwd()}")
      log_error(
          f"Please check your --ingester_configs parameter: {state.args.ingester_configs}"
      )
      return 1

  # reload(state)
  tsdb_class = get_adapter_class(state.args.tsdb_adapter)
  if not tsdb_class:
    available_adapters = get_available_adapters()
    if available_adapters:
      raise ValueError(
          f"Unsupported TSDB_ADAPTER adapter: {state.args.tsdb_adapter}. "
          f"Available adapters: {', '.join(available_adapters)}. "
          f"You may need to install additional dependencies for other adapters."
      )
    else:
      raise ValueError(
          "No database adapters are available! "
          "Please check your installation and ensure required dependencies are installed."
      )

  state.tsdb.set_adapter(await tsdb_class.connect())

  # ping dbs for readiness checks
  if state.args.ping:
    db_ok, cache_ok = await gather(state.tsdb.ping(), state.redis.ping())
    log_info(f"Connected to DB: {db_ok}, Cache: {cache_ok}")
    await state.tsdb.close()
    await state.redis.close()
    return (0 if (db_ok and cache_ok) else 1
            )  # exit with 0 if both dbs are ok, 1 otherwise

  # start server or ingester
  try:
    config = state.config  # ConfigProxy acts as the config object directly
    await (start_server(config)
           if state.args.server else start_ingester(config))
  except KeyboardInterrupt:
    log_info("Shutting down...")
  finally:
    await state.tsdb.close()
    await state.redis.close()


if __name__ == "__main__":
  log_info(f"""
        __
   ____/ /  ___  __ _  ___
  / __/ _ \\/ _ \\/  ' \\/ _ \\
  \\__/_//_/\\___/_/_/_/ .__/
           ingester /_/ v{state.meta.version}\n\n""")
  ap = ArgParser(
      description=
      "Chomp retrieves, transforms and archives data from various sources.")
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
              f"chomp-{generate_hash(length=8)}",
              None,
              "Unique instance identifier",
          ),
          (
              ("-r", "--max_retries"),
              int,
              5,
              None,
              "Max ingester retries per event, applies to fetching/querying",
          ),
          (
              ("-rc", "--retry_cooldown"),
              int,
              2,
              None,
              "Min sleep time between retries, in seconds",
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
          (
              ("-m", "--monitored"),
              bool,
              False,
              "store_true",
              "Enable monitoring for all ingesters",
          ),
          (
              ("-uidf", "--uid_masks_file"),
              str,
              "uid-masks",
              None,
              "Path to UID masks file for instance naming",
          ),
      ],
      "Ingester runtime": [
          (
              ("-p", "--perpetual_indexing"),
              bool,
              False,
              "store_true",
              "Perpetually listen for new blocks to index, requires capable RPCs",
          ),
          (
              ("-j", "--max_jobs"),
              int,
              15,
              None,
              "Max ingester jobs to run concurrently",
          ),
          (
              ("-c", "--ingester_configs"),
              str,
              "./ingesters.yml",
              None,
              "Comma-delimited list of ingester YAML configuration files",
          ),
      ],
      "Server runtime": [
          (
              ("-s", "--server"),
              bool,
              False,
              "store_true",
              "Run as server (ingester by default)",
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
  run(main(ap))
