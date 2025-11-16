from asyncio import gather, run, sleep
import secrets
from typing import Type, Optional
from pathlib import Path

from src.utils import (
    log_info,
    log_warn,
    log_error,
    ArgParser,
    prettify,
)
from src import state
from src.models import IngesterConfig, Tsdb, TsdbAdapter


def get_adapter_class(adapter: TsdbAdapter) -> Optional[Type[Tsdb]]:
  """
    Dynamically load database adapter classes, only including those that can be imported.
    This allows the system to work with different database dependencies without breaking.
    """
  from src.adapters import get_adapter

  return get_adapter(adapter)


def get_available_adapters() -> list[str]:
  """
    Get a list of all available database adapters that can be imported.
    Useful for debugging and providing helpful error messages.
    """
  from src.adapters import get_adapter

  adapter_names = [
      "tdengine", "sqlite", "clickhouse", "duckdb", "timescale", "questdb",
      "mongodb", "influxdb", "victoriametrics", "kx"
  ]

  available = []
  for name in adapter_names:
    if get_adapter(name):
      available.append(name)

  return sorted(available)


async def start_ingester(config: IngesterConfig):
  # ingester specific imports
  from src.cache import is_task_claimed, ping as redis_ping, register_ingester, register_instance
  from src.actions import schedule, scheduler

  # Skip Redis validation in test mode
  if not state.args.test_mode:
    # Validate Redis connection before proceeding
    try:
      redis_connected = await redis_ping()
      if not redis_connected:
        log_error(
            "Failed to connect to Redis. Cannot proceed with task claiming.")
        return
      log_info("Redis connection verified")
    except Exception as e:
      log_error(f"Redis connection error: {e}")
      return
  else:
    log_info("TEST MODE: Skipping Redis connection")

  ingesters = config.ingesters

  # Validate configuration loading
  if not ingesters:
    log_error(
        "No ingesters found in configuration! Check your configuration files.")
    return

  log_info(f"Loaded {len(ingesters)} ingester configurations: " + ", ".join([
      f"{ingester.name} ({ingester.ingester_type}, {ingester.interval}, {len(ingester.fields)} fields)"
      for ingester in ingesters
  ]))

  # Skip registration in test mode
  if not state.args.test_mode:
    # Register all ingesters with full configurations
    log_info("Registering ingesters in resource registry...")
    registration_tasks = [register_ingester(ing) for ing in ingesters]
    registration_results = await gather(*registration_tasks,
                                        return_exceptions=True)
    successful_registrations = sum(1 for r in registration_results if r is True)
    log_info(
        f"Successfully registered {successful_registrations}/{len(ingesters)} ingesters: {', '.join([ing.name for ing in ingesters])}"
    )

    # Register this instance
    log_info("Registering instance...")
    instance_registered = await register_instance(state.instance)
    if instance_registered:
      log_info(f"Instance {state.instance.name} registered successfully")
    else:
      log_warn(f"Failed to register instance {state.instance.name}")

  # Instance initialization is handled in state.init() during startup

  # Add system monitor ingester if monitored flag is set
  if state.args.monitored:
    from src.models import InstanceMonitor
    monitor_ingester = InstanceMonitor(state.instance)
    ingesters.append(monitor_ingester)
    # Register the monitor ingester too
    await register_ingester(monitor_ingester)
    log_info("✅ System monitor ingester added to schedule and registered")

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
    for ing in ingesters:
      if ing in in_range:
        claims += 1
      claim_str = f"({claims}/{state.args.max_jobs})"
      table_data.append([
          ing.name[:21].ljust(24, ".") if len(ing.name) > 24 else ing.name,
          ing.ingester_type,
          ing.interval,
          len(ing.fields),
          "yes 🟢" if ing in unclaimed else "no 🔴",
          f"yes 🟢 {claim_str}" if ing in in_range else "no 🔴",
      ])

    log_info(
        f"\n{prettify(table_data, ['Resource', 'Ingester', 'Interval', 'Fields', 'Claimable', 'Picked-up'])}"
    )

    # claim tasks before scheduling - handle failures gracefully
    successfully_claimed = []
    for ing in in_range:
      try:
        await ensure_claim_task(ing)
        successfully_claimed.append(ing)
        log_info(f"Successfully claimed task: {ing.name}")
      except ValueError as e:
        log_warn(f"Failed to claim task {ing.name}: {e}")
        continue
      except Exception as e:
        log_error(f"Unexpected error claiming task {ing.name}: {e}")
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

  # TEST MODE: Run each ingester once and exit
  if state.args.test_mode:
    log_info(f"\n{'='*80}")
    log_info("TEST MODE: Running single collection epoch for each ingester")
    log_info(f"{'='*80}\n")

    for ing in successfully_claimed:
      log_info(f"\n{'='*60}")
      log_info(f"Testing {ing.name} ({ing.ingester_type})")
      log_info(f"Target: {ing.target if hasattr(ing, 'target') else 'N/A'}")
      log_info(f"Fields: {len(ing.fields)}")
      log_info(f"{'='*60}")

      try:
        # Get the scheduler function for this ingester type
        from src.actions.schedule import get_scheduler
        scheduler_fn = get_scheduler(ing.ingester_type)

        if scheduler_fn:
          # Schedule the ingester (sets up the ingest function)
          await scheduler_fn(ing)

          # Get the ingest function from scheduler
          if ing.id in scheduler.job_by_id:
            ingest_fn, args = scheduler.job_by_id[ing.id]

            # Run the ingest function once
            log_info(f"Running single ingest epoch for {ing.name}...")
            await ingest_fn(*args)

            # Log the collected data - show ALL fields
            log_info(f"\n{'='*80}")
            log_info(f"COLLECTED DATA FOR: {ing.name}")
            log_info(f"{'='*80}")

            # Count fields with values
            fields_with_values = sum(1 for f in ing.fields if f.value is not None)
            log_info(f"Total fields: {len(ing.fields)} | With values: {fields_with_values} | None: {len(ing.fields) - fields_with_values}")
            log_info(f"{'─'*80}")

            # Log all fields (no truncation for test mode)
            for idx, field in enumerate(ing.fields, 1):
              value_str = str(field.value) if field.value is not None else "None"
              log_info(f"  [{idx:3d}] {field.name:30} = {value_str}")
            log_info(f"{'='*80}\n")
          else:
            log_warn(f"No job found in scheduler for {ing.name}")
        else:
          log_error(f"No scheduler found for ingester type: {ing.ingester_type}")

      except Exception as e:
        log_error(f"Error testing {ing.name}: {e}")
        import traceback
        log_error(traceback.format_exc())

    log_info(f"\n{'='*80}")
    log_info("TEST MODE: Completed all test runs. Exiting...")
    log_info(f"{'='*80}\n")
    return

  # NORMAL MODE: schedule and run continuously
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
    # NB: Individual schedulers use deferred start pattern (start=False)
    # so immediate tasks may be empty even when scheduling is successful
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


async def start_server():
  # server specific imports
  from src.server import start
  from src.cache import register_instance

  # Register this server instance
  log_info("Registering server instance...")
  instance_registered = await register_instance(state.instance)
  if instance_registered:
    log_info(
        f"✅ Server instance {state.instance.name} registered successfully")
  else:
    log_warn(f"⚠️ Failed to register server instance {state.instance.name}")

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
  # Skip database connection in test mode
  if not state.args.test_mode:
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
  else:
    log_info("TEST MODE: Skipping database connection")

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
    if state.args.server:
      await start_server()
    else:
      await start_ingester(state.ingester_config)
  except KeyboardInterrupt:
    log_info("Shutting down...")
  finally:
    # Skip closing connections in test mode (they were never opened)
    if not state.args.test_mode:
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
              f"chomp-{secrets.token_urlsafe(8)}",
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
              ("-it", "--ingestion_timeout"),
              int,
              3,
              None,
              "RPC/HTTP request timeout for data ingestion, in seconds",
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
              6,
              None,
              "Max concurrent resources ingested by this instance",
          ),
          (
              ("-c", "--ingester_configs"),
              str,
              "./ingesters.yml",
              None,
              "Comma-delimited list of ingester YAML configuration files",
          ),
          (
              ("-test", "--test_mode"),
              bool,
              False,
              "store_true",
              "Test mode: run single epoch, log data, skip persistence",
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
          (
              ("-sc", "--server_config"),
              str,
              "./server-config.yml",
              None,
              "Server configuration YAML file",
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
