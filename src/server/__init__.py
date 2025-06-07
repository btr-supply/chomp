from fastapi import FastAPI
from fastapi.concurrency import asynccontextmanager
from starlette.middleware.gzip import GZipMiddleware
from starlette.middleware.cors import CORSMiddleware
import uvicorn

from .. import state
from .responses import ROUTER_ERROR_HANDLERS, ApiResponse
from .routers import forwarder, retriever, admin
from .middlewares import Limiter


@asynccontextmanager
async def lifespan(app: FastAPI):
  # pre-startup
  yield
  # post-shutdown


# TODO: these should be configurable as well as endpoints ppr
DEFAULT_LIMITS = {
    'whitelist': ['0.0.0.0', '127.0.0.1', 'localhost'],
    'rpm': 60,
    'rph': 1200,
    'rpd': 9600,  # 1/.3/.1 logarithmic req caps (burst protection)
    'spm': 1e8,
    'sph': 2e9,
    'spd': 1.6e10,  # 1/.3/.1 logarithmic bandwidth caps (burst protection)
    'ppm': 60,
    'pph': 1200,
    'ppd': 9600,  # 1/.3/.1 logarithmic points caps (burst protection)
}


async def start():
  app = FastAPI(
      lifespan=lifespan,
      default_response_class=ApiResponse,
      exception_handlers=ROUTER_ERROR_HANDLERS,  # type: ignore[arg-type]
      version=state.meta.version,
      title=state.meta.name,
      description=state.meta.description,
      redoc_url=None,  # disable redoc (OpenAPI generation bug)
      docs_url=None,  # disable docs (OpenAPI generation bug)
      openapi_url=None,  # disable openapi (OpenAPI generation bug)
  )
  state.server = app
  for base in [
      "", f"/v{state.meta.version}", f"/v{state.meta.major_version}",
      f"/v{state.meta.major_version}.{state.meta.minor_version}"
  ]:  # /v{latest} aliasing to /
    include_in_schema = len(base) == 3
    app.include_router(retriever.router,
                       prefix=base,
                       include_in_schema=include_in_schema)  # http_api
    app.include_router(forwarder.router,
                       prefix=base,
                       include_in_schema=include_in_schema)  # ws_api
    app.include_router(admin.router,
                       prefix=f"{base}/admin",
                       include_in_schema=include_in_schema)  # admin_api

  # app.add_middleware(VersionResolver) # redirects /v{latest} to /
  app.add_middleware(
      Limiter, **DEFAULT_LIMITS
  )  # type: ignore[arg-type]  # rate limiting (req count/bandwidth/points)
  app.add_middleware(GZipMiddleware,
                     minimum_size=int(1e3))  # only compress responses > 1kb
  app.add_middleware(
      CORSMiddleware,
      allow_origins=[],
      allow_origin_regex=
      r"^https?://localhost(:[0-9]+)?$|^https?://127\.0\.0\.1(:[0-9]+)?$|^wss?://localhost(:[0-9]+)?$|^wss?://127\.0\.0\.1(:[0-9]+)?$|^https?://[^/]+\.btr\.markets$|^wss?://[^/]+\.btr\.markets$|^https?://[^/]+\.btr\.supply$|^wss?://[^/]+\.btr\.supply$|^https?://[^/]+\.astrolab\.fi$|^wss?://[^/]+\.astrolab\.fi$",
      allow_credentials=True,
      allow_methods=["*"],
      allow_headers=["*"])

  config = uvicorn.Config(app,
                          host=state.args.host,
                          port=state.args.port,
                          ws_ping_interval=state.args.ws_ping_interval,
                          ws_ping_timeout=state.args.ws_ping_timeout,
                          log_config=None)  # defaults to using utils.logger

  server = uvicorn.Server(config)
  await server.serve()


# Note: main() has been moved to __main__.py for proper argument initialization
