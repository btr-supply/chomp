from os import environ as env
from typing import Optional

from ..utils import log_info
from .prometheus import PrometheusAdapter


class VictoriaMetrics(PrometheusAdapter):
  """
  VictoriaMetrics adapter extending the Prometheus adapter.
  VictoriaMetrics is largely compatible with Prometheus API.
  """

  def __init__(self, host: str, port: int, db: str, user: str, password: str):
    super().__init__(host, port, db, user, password)

  @classmethod
  async def connect(cls,
                    host: Optional[str] = None,
                    port: Optional[int] = None,
                    db: Optional[str] = None,
                    user: Optional[str] = None,
                    password: Optional[str] = None) -> "VictoriaMetrics":
    """Factory method to create and connect to VictoriaMetrics."""
    self = cls(host=host or env.get("VICTORIAMETRICS_HOST") or "localhost",
               port=int(port or env.get("VICTORIAMETRICS_PORT") or 8428),
               db=db or env.get("VICTORIAMETRICS_DB") or "default",
               user=user or env.get("DB_RW_USER") or "",
               password=password or env.get("DB_RW_PASS") or "")
    await self.ensure_connected()
    return self

  async def ensure_connected(self):
    """Establish connection to VictoriaMetrics."""
    # Connection is handled by singleton HTTP client
    log_info(f"Connected to VictoriaMetrics on {self.host}:{self.port}")

  def _setup_urls(self):
    super()._setup_urls()
    # VictoriaMetrics specific endpoints
    self.export_url = f"{self.base_url}/export"
    self.import_csv_url = f"{self.base_url}/import/csv"

  # VictoriaMetrics uses the same API endpoints as Prometheus, so no need to override _setup_urls()
  # All other methods are inherited from PrometheusAdapter

  async def fetch_batch_by_ids(self, table: str,
                               uids: list[str]) -> list[tuple]:
    """Fetch multiple records by their UIDs from VictoriaMetrics

    Uses the same implementation as Prometheus since VictoriaMetrics is compatible
    """
    # Delegate to parent Prometheus implementation
    return await super().fetch_batch_by_ids(table, uids)
