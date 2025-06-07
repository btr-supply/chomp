import re
from uuid import uuid4
from typing import Awaitable, Tuple, Any, TypeVar
from functools import lru_cache
from fastapi import Response
from fastapi.exceptions import RequestValidationError, ValidationException, HTTPException, WebSocketException
import orjson
from ..utils import log_error
from ..model import DataFormat

ORJSON_OPTIONS = (orjson.OPT_SERIALIZE_NUMPY | orjson.OPT_SERIALIZE_DATACLASS
                  | orjson.OPT_SERIALIZE_UUID | orjson.OPT_SERIALIZE_NUMPY)


class ApiResponse(Response):
  media_type = "application/json"
  data_format: DataFormat = "json:row"

  def __init__(self,
               content=None,
               data_format: DataFormat = "json:row",
               **kwargs) -> None:

    if not data_format.startswith("json"):
      # Set appropriate media type based on format
      match data_format:
        case "csv":
          self.media_type = "text/csv"
        case "tsv":
          self.media_type = "text/plain"
        case "psv":
          self.media_type = "text/pipe-separated-values"
        case "parquet":
          self.media_type = "application/vnd.apache.parquet"
        case "arrow" | "feather":
          self.media_type = "application/vnd.apache.arrow.file"
        # case "orc":
        #     self.media_type = "application/vnd.apache.orc" # not supported
        case "avro":
          self.media_type = "application/vnd.apache.avro"
        case _:
          self.media_type = "application/octet-stream"  # Default to binary for unknown formats

    super().__init__(content=content, **kwargs)
    self.headers["Content-Type"] = f"{self.media_type}; charset=utf-8"
    self.data_format = data_format

  def render(self, content: Any) -> bytes:
    if isinstance(content, bytes):
      return content
    if isinstance(content, str):
      return content.encode('utf-8')
    match self.data_format:
      case "json:row" | "json:column":
        return orjson.dumps(content, option=ORJSON_OPTIONS)
      case "csv" | "tsv" | "psv":
        return content.encode(
            'utf-8')  # Assuming content is already in CSV format
      case "parquet" | "arrow" | "feather" | "orc" | "avro":
        return content  # Assuming content is already in Parquet format
      case _:
        raise ValueError(f"Unsupported response format: {self.data_format}")


_ERROR_PATTERNS = [
    (re.compile(r"(?i)not\s*found|missing|404"), 404),
    (re.compile(r"(?i)unauthorized|forbidden|403"), 403),
    (re.compile(r"(?i)exceeded|too\s*many|limit\s*reached|429"), 429),
    (re.compile(
        r"(?i)formatting\s*error|invalid\s*format|non\s*processable|invalid\s*field|literal|422"
    ), 422),
    (re.compile(r"(?i)bad\s*request|invalid|400"), 400),
    (re.compile(r"(?i)server\s*error|unexpected|500"), 500),
]


@lru_cache(maxsize=1024)
def _get_error_code(err_msg: str) -> int:
  if err_msg:
    for pattern, code in _ERROR_PATTERNS:
      if pattern.search(err_msg):
        return code
  return 400


class ApiError(HTTPException):

  def __init__(self,
               error_msg=None,
               status_code=None,
               headers=None,
               trace_id=None):

    status_code = status_code or _get_error_code(error_msg)
    error_msg = error_msg or "Bad Request"
    detail = error_msg
    if status_code >= 500:
      detail = "An unexpected error occurred. Please try again later. If the issue persists, please contact the team."
    self.status_code = status_code
    self.detail = detail
    self.trace_id = trace_id or uuid4()
    self.headers = headers or {}
    self.headers["Trace-ID"] = str(self.trace_id)
    log_error(f"Trace [{self.trace_id}]: {error_msg}")
    super().__init__(status_code=status_code, detail=detail)

  def to_response(self):
    return ApiResponse(status_code=self.status_code,
                       content={
                           "code": self.status_code,
                           "message": self.detail,
                           "trace_id": self.trace_id
                       },
                       headers=self.headers)

  def __str__(self) -> str:
    return self.to_response().body.decode("utf-8")


T = TypeVar('T')


async def handle_service_error(
    service_call: Awaitable[Tuple[str, Any]]) -> Any:
  err, result = await service_call
  if err:
    raise ApiError(error_msg=err)
  return result


def router_error_handler(request, exc):
  # Create ApiError from the exception if it isn't already one
  if not isinstance(exc, ApiError):
    exc = ApiError(status_code=getattr(exc, "status_code", None),
                   error_msg=str(exc))
  return exc.to_response()


ROUTER_ERROR_HANDLERS = {
    RequestValidationError: router_error_handler,
    ValidationException: router_error_handler,
    HTTPException: router_error_handler,
    WebSocketException: router_error_handler,
    Exception: router_error_handler,
}
