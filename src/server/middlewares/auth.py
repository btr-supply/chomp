from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response

from ..responses import ApiError
from ...services.auth import AuthService
from ...models.base import SCOPES, Scope
from ..routes import Route
from ...services.loader import parse_resources_fields


class AuthMiddleware(BaseHTTPMiddleware):
    def __init__(self, app):
        super().__init__(app)
        # Pre-compile route patterns for better performance
        self._route_patterns = [
            (route, self._compile_route_pattern(route.value.router_prefix, route.value.endpoint))
            for route in Route
        ]

    def _compile_route_pattern(self, prefix: str, endpoint: str) -> tuple[list[str], bool]:
        """Pre-compile route pattern into parts and whether it has parameters."""
        full_path = f"{prefix}{endpoint}" if prefix else endpoint
        parts = full_path.split('/')
        has_params = any(part.startswith('{') and part.endswith('}') for part in parts)
        return parts, has_params

    def _find_matching_route(self, path: str):
        """Optimized route matching with pre-compiled patterns and version handling."""

        path_parts = path.split('/')

        for route, (route_parts, has_params) in self._route_patterns:
            if len(route_parts) != len(path_parts):
                continue

            if not has_params:
                # Fast exact match for routes without parameters
                if path == '/'.join(route_parts):
                    return route
            else:
                # Parameter matching
                if all(
                    route_part == path_part or (route_part.startswith('{') and route_part.endswith('}'))
                    for route_part, path_part in zip(route_parts, path_parts)
                ):
                    return route
        return None

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        try:
            # 1. Load user and set admin status
            user = await AuthService.load_request_user(request)
            request.state.user = user
            is_admin = user and user.is_admin()

            # 2. Check route protection
            current_route = self._find_matching_route(request.url.path)
            if current_route and current_route.protected and not is_admin:
                return ApiError(error_msg="Forbidden", status_code=403)

            # 3. Parse and validate scope
            params = dict(request.query_params)
            scope_name = params.get('scope', '').lower()
            requested_scope = SCOPES.get(scope_name, Scope.DEFAULT)

            if not is_admin and requested_scope and \
              requested_scope not in {Scope.DEFAULT, Scope.DETAILED}:
                return ApiError(error_msg="Forbidden", status_code=403)

            request.state.scope = requested_scope

            # 4. Parse resources and fields efficiently
            resources_str = (
                request.path_params.get("resources") or
                request.path_params.get("resource") or
                params.get("resources", "")
            )
            fields_str = params.get("fields") or request.path_params.get("fields") or "*"

            if resources_str:
                request.state.resources, request.state.fields = \
                  await parse_resources_fields(
                      resources_str, fields_str, request.state.scope, request)
            else:
                request.state.resources = []
                request.state.fields = []

            return await call_next(request)

        except (ValueError, PermissionError) as e:
            return ApiError(error_msg=str(e), status_code=403)
        except Exception:
            return ApiError(error_msg="Internal Server Error", status_code=500)
