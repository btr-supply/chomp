# System monitoring ingester for collecting instance vitals
# Provides CPU, memory, disk usage monitoring for chomp instances

from asyncio import Task
import psutil
import time

from ..actions.schedule import scheduler
from ..actions.store import transform_and_store
from ..utils import log_debug, log_error
from ..cache import ensure_claim_task
from ..model import Ingester
from .. import state

# Initialize CPU measurement (required for non-blocking calls)
try:
    psutil.cpu_percent(interval=None)  # Initialize CPU measurement
except Exception:
    pass


async def schedule(c: Ingester) -> list[Task]:
    """Schedule monitor ingester for system vitals collection"""

    async def ingest(c: Ingester):
        """Collect and store system vitals"""
        await ensure_claim_task(c)

        try:
            current_time = time.time()

            # Non-blocking CPU measurement
            cpu_usage = 0.0
            try:
                cpu_usage = psutil.cpu_percent(
                    interval=None
                )  # Non-blocking after initialization
            except Exception as e:
                log_error(f"Failed to get CPU usage: {e}")

            # Memory usage in bytes
            memory_usage = 0.0
            try:
                memory_usage = psutil.virtual_memory().used
            except Exception as e:
                log_error(f"Failed to get memory usage: {e}")

            # Disk I/O rate calculation (bytes per second)
            disk_usage = 0.0
            try:
                disk_io = psutil.disk_io_counters()
                current_disk_bytes = disk_io.read_bytes + disk_io.write_bytes

                if hasattr(state, "_last_disk_bytes") and hasattr(
                    state, "_last_disk_time"
                ):
                    time_delta = current_time - state._last_disk_time  # type: ignore[attr-defined]
                    if time_delta > 0:
                        disk_bytes_delta = abs(
                            current_disk_bytes - state._last_disk_bytes
                        )
                        disk_usage = disk_bytes_delta / time_delta  # bytes per second

                state._last_disk_bytes = current_disk_bytes  # type: ignore[attr-defined]
                state._last_disk_time = current_time  # type: ignore[attr-defined]
            except Exception as e:
                log_error(f"Failed to get disk I/O: {e}")

            # Update ingester fields with collected data
            # Handle case where state.instance might be None
            instance = state.instance
            field_values = {
                "ts": None,  # Will be set automatically by transform_and_store
                "instance_name": instance.name if instance else "",
                "ipv4": instance.ipv4 if instance else "",
                "ipv6": instance.ipv6 if instance else "",
                "resources_count": instance.resources_count if instance else 0,
                "cpu_usage": cpu_usage,
                "memory_usage": memory_usage,
                "disk_usage": disk_usage,
                # Geolocation data (transient fields)
                "coordinates": instance.coordinates if instance else "",
                "timezone": instance.timezone if instance else "",
                "country_code": instance.country_code if instance else "",
                "location": instance.location if instance else "",
                "isp": instance.isp if instance else "",
            }

            for field in c.fields:
                if field.name in field_values and field_values[field.name] is not None:
                    field.value = field_values[field.name]

            # Use standard store flow like all other ingesters
            await transform_and_store(c)

            if state.args.verbose:
                memory_mb = (
                    float(field_values["memory_usage"]) / 1024 / 1024
                    if field_values["memory_usage"]
                    else 0.0
                )
                log_debug(
                    f"Stored instance vitals: resources={instance.resources_count if instance else 0}, cpu={field_values['cpu_usage']:.1f}%, mem={memory_mb:.1f}MB, disk_io={field_values['disk_usage']:.1f}B/s"
                )

        except Exception as e:
            log_error(f"Failed to collect instance vitals: {e}")

    # Register/schedule the ingester
    task = await scheduler.add_ingester(c, fn=ingest, start=False)
    return [task] if task is not None else []
