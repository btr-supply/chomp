from os import environ as env
import aiohttp

from ..utils import log_info
from .prometheus import PrometheusAdapter

class VictoriaMetrics(PrometheusAdapter):
  """
  VictoriaMetrics adapter that extends the Prometheus adapter.
  VictoriaMetrics is fully compatible with Prometheus API.
  """

  @classmethod
  async def connect(
    cls,
    host: str | None = None,
    port: int | None = None,
    db: str | None = None,
    user: str | None = None,
    password: str | None = None
  ) -> "VictoriaMetrics":
    self = cls(
      host=host or env.get("VICTORIAMETRICS_HOST") or "localhost",
      port=int(port or env.get("VICTORIAMETRICS_PORT") or 8428),
      db=db or env.get("VICTORIAMETRICS_DB") or "default",
      user=user or env.get("DB_RW_USER") or "",
      password=password or env.get("DB_RW_PASS") or ""
    )
    await self.ensure_connected()
    return self

  async def ensure_connected(self):
    if not self.session:
      # Create session with basic auth if credentials provided
      auth = None
      if self.user and self.password:
        auth = aiohttp.BasicAuth(self.user, self.password)

      self.session = aiohttp.ClientSession(auth=auth)
      log_info(f"Connected to VictoriaMetrics on {self.host}:{self.port}")

  # VictoriaMetrics uses the same API endpoints as Prometheus, so no need to override _setup_urls()
  # All other methods are inherited from PrometheusAdapter
