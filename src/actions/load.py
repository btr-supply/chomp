from datetime import datetime, timezone
from typing import Optional
import numpy as np

from ..model import Ingester
from ..cache import get_cache
from .. import state

UTC = timezone.utc

async def load(c: Ingester, from_date: datetime, to_date: Optional[datetime], aggregation_interval=None) -> Ingester|list|np.ndarray:
  if not to_date:
    to_date = datetime.now(UTC)
  if c.resource_type == "value":
    return await load_one(c)
  if not aggregation_interval:
    aggregation_interval = c.interval
  return await state.tsdb.fetch(c.name, from_date, to_date, aggregation_interval)

async def load_one(c: Ingester) -> Ingester|list|np.ndarray:
  return c.load_values(await get_cache(c.id))
