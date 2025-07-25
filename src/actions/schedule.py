from asyncio import gather, get_running_loop, Task
from aiocron import Cron, crontab
from typing import Callable, Any, cast, Optional

from ..cache import ensure_claim_task, inherit_fields
from ..utils import log_info, log_error, submit_to_threadpool,\
  Interval, interval_to_cron
from ..models.base import IngesterType
from ..models.ingesters import Ingester
from .. import state

# Type annotation for function with attributes
scheduler_registry: dict[IngesterType, Callable] = {}


def get_scheduler(ingestor_type: IngesterType) -> Optional[Callable]:
  global scheduler_registry
  if not scheduler_registry:  # singleton
    from .. import ingesters

    # Core modules (always available)
    scheduler_registry = {
        "http_api": ingesters.http_api.schedule,
        "ws_api": ingesters.ws_api.schedule,
        "processor": ingesters.processor.schedule,
    }

    # Optional modules - add if available
    optional_modules = [
        "static_scrapper", "evm_caller", "evm_logger", "svm_caller",
        "sui_caller"
    ]

    for module_name in optional_modules:
      if hasattr(ingesters, module_name):
        # Map some module names to different scheduler keys
        scheduler_key = "scrapper" if module_name == "static_scrapper" else module_name
        scheduler_registry[cast(IngesterType, scheduler_key)] = getattr(
            ingesters, module_name).schedule

  return scheduler_registry.get(ingestor_type, None)


async def monitor_cron(cron: Cron):
  """Monitor a cron job and handle exceptions"""
  while True:
    try:
      await cron.next()
    except Exception as e:
      log_error(f"Cron job failed with exception: {e}")
      get_running_loop().stop()
      break


class Scheduler:

  def __init__(self):
    self.cron_by_job_id: dict[str, Cron] = {}
    self.cron_by_interval: dict[Interval, Cron] = {}
    self.jobs_by_interval: dict[Interval, list[str]] = {}
    self.job_by_id: dict[str, tuple[Callable, tuple]] = {}

  def run_threaded(self, job_ids: list[str]) -> list[Any]:
    jobs = [self.job_by_id[j] for j in job_ids]
    ft = [
        submit_to_threadpool(state.thread_pool, job[0], *job[1])
        for job in jobs
    ]
    return [f.result() for f in ft]  # wait for all results

  async def run_async(self, job_ids: list[str]) -> list[Any]:
    jobs = [self.job_by_id[j] for j in job_ids]
    ft = [job[0](*job[1]) for job in jobs]
    return await gather(*ft)

  async def add(self,
                id: str,
                fn: Callable,
                args: tuple,
                interval: Interval = "h1",
                start=True,
                threaded=False) -> Optional[Task]:
    if id in self.job_by_id:
      raise ValueError(f"Duplicate job id: {id}")
    self.job_by_id[id] = (fn, args)

    jobs = self.jobs_by_interval.setdefault(interval, [])
    jobs.append(id)

    if not start:
      return None

    return await self.start_interval(interval, threaded)

  async def start_interval(self, interval: Interval, threaded=False) -> Task:
    if interval in self.cron_by_interval:
      old_cron = self.cron_by_interval[interval]
      old_cron.stop()  # stop prev cron

    job_ids = self.jobs_by_interval[interval]

    cron = crontab(interval_to_cron(interval),
                   func=self.run_async,
                   args=(job_ids, ))

    # update the interval's jobs cron ref
    for id in job_ids:
      self.cron_by_job_id[id] = cron
    self.cron_by_interval[interval] = cron

    log_info(
        f"Proc {state.args.proc_id} starting {interval} {'threaded' if threaded else 'async'} cron with {len(job_ids)} jobs: {job_ids}"
    )
    return await monitor_cron(self.cron_by_job_id[id])

  async def start(self, threaded=False) -> list[Task]:
    intervals, jobs = self.jobs_by_interval.keys(), self.job_by_id.keys()
    log_info(
        f"Starting {len(jobs)} jobs ({len(intervals)} crons: {list(intervals)})"
    )
    return await gather(*[
        self.start_interval(i, threaded) for i in self.jobs_by_interval.keys()
    ])

  async def add_ingester(self,
                         ing: Ingester,
                         fn: Callable,
                         start=True,
                         threaded=False) -> Optional[Task]:
    return await self.add(id=ing.id,
                          fn=fn,
                          args=(ing, ),
                          interval=ing.interval,
                          start=start,
                          threaded=threaded)

  async def add_ingesters(self,
                          ingesters: list[Ingester],
                          fn: Callable,
                          start=True,
                          threaded=False) -> Optional[list[Task]]:
    intervals = set([c.interval for c in ingesters])
    await gather(*[
        self.add_ingester(c, fn, start=False, threaded=threaded)
        for c in ingesters
    ])
    if start:
      return await gather(
          *[self.start_interval(i, threaded) for i in intervals])
    return None


async def schedule(ing: Ingester) -> list[Task]:
  if ing.ingester_type == "processor":
    ing = await inherit_fields(ing)
  # Registry registration is now handled automatically via monitoring
  schedule_fn = get_scheduler(ing.ingester_type)
  if not schedule_fn:
    raise ValueError(
        f"Unsupported ingester type: {ing.ingester_type} (available: {list(scheduler_registry.keys())})"
    )
  await ensure_claim_task(ing)
  tasks = await schedule_fn(ing)
  log_info(
      f"Scheduled for ingestion: {ing.name}.{ing.interval} [{', '.join([field.name for field in ing.fields])}]"
  )
  return tasks


scheduler = Scheduler()
