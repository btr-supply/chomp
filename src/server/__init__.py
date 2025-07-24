from fastapi import FastAPI
from fastapi.concurrency import asynccontextmanager
from starlette.middleware.gzip import GZipMiddleware
from starlette.middleware.cors import CORSMiddleware
import uvicorn

from .. import state
from .responses import ROUTER_ERROR_HANDLERS, ApiResponse
from .routers import auth, forwarder, retriever, admin, config
from .middlewares.version_resolver import VersionResolver
from .middlewares.limiter import RateLimitMiddleware
from .middlewares.auth import AuthMiddleware
# Protection now handled by @protected decorator


@asynccontextmanager
async def lifespan(app: FastAPI):
  # pre-startup
  yield
  # post-shutdown


async def start():
  app = FastAPI(
      lifespan=lifespan,
      default_response_class=ApiResponse,
      exception_handlers=ROUTER_ERROR_HANDLERS,  # type: ignore[arg-type]
      version=state.meta.version,
      title=state.meta.name,
      description=state.meta.description,
      # Re-enable OpenAPI endpoints to test if the bug is fixed
      # redoc_url=None,  # disable redoc (OpenAPI generation bug)
      # docs_url=None,  # disable docs (OpenAPI generation bug)
      # openapi_url=None,  # disable openapi (OpenAPI generation bug)
  )
  state.server = app
  for base in [
      "", f"/v{state.meta.version}", f"/v{state.meta.major_version}",
      f"/v{state.meta.major_version}.{state.meta.minor_version}"
  ]:  # /v{latest} aliasing to /
    include_in_schema = len(base) == 3
    app.include_router(auth.router,
                       prefix=base,
                       tags=["Authentication"],
                       include_in_schema=include_in_schema)
    app.include_router(forwarder.router,
                       prefix=base,
                       tags=["Websockets"],
                       include_in_schema=include_in_schema)
    app.include_router(retriever.router,
                       prefix=base,
                       tags=["Data Retrieval"],
                       include_in_schema=include_in_schema)
    app.include_router(admin.router,
                       prefix=f"{base}/admin",
                       tags=["Administration"],
                       include_in_schema=include_in_schema)
    app.include_router(config.router,
                       prefix=f"{base}/config",
                       tags=["Configuration"],
                       include_in_schema=include_in_schema)

  # app.add_middleware(VersionResolver) # redirects /v{latest} to /
  # Route protection now handled by the new AuthMiddleware
  app.add_middleware(AuthMiddleware)
  app.add_middleware(RateLimitMiddleware)
  app.add_middleware(GZipMiddleware,
                     minimum_size=int(1e3))  # only compress responses > 1kb
  app.add_middleware(CORSMiddleware,
                     allow_origins=[],
                     allow_origin_regex=state.server_config.allow_origin_regex,
                     allow_credentials=True,
                     allow_methods=["*"],
                     allow_headers=["*"])

  # Create server configuration
  server_config = uvicorn.Config(
      app=app,
      host=state.server_config.host,
      port=state.server_config.port,
      access_log=False,
      server_header=False,
  )

  # Start the server
  server = uvicorn.Server(server_config)
  await server.serve()


# NB: main() has been moved to __main__.py for proper argument initialization
