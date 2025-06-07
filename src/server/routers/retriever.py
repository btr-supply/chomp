from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse
from typing import Optional

from ...model import DataFormat, Interval, to_scope_mask
from ...server.middlewares.limiter import limit
from ...server.responses import ApiResponse
from ...server.responses import handle_service_error as hse
from ...services import converter, gatekeeeper, limiter, loader,\
  ts_analysis, status_checker
from ...utils import fit_date_params, load_template, split

router = APIRouter()

_index_html = load_template("index.html")
_docs_html = load_template("docs.html")


@router.get("/")
@router.get("/index")
async def get_root():
  return HTMLResponse(_index_html)


@router.get("/docs")
async def get_docs():
  return HTMLResponse(_docs_html)


@router.get("/ping")
@limit(points=1)
async def ping(req: Request,
               utc_time: int = Query(
                   None, description="Client UTC timestamp in milliseconds")):
  return ApiResponse(await hse(status_checker.ping(req, utc_time)))


@router.get("/resources")
@router.get("/schema")
@router.get("/schema/{resources:path}")
@limit(points=1)
async def get_schema(req: Request,
                     resources: Optional[str] = None,
                     fields: Optional[str] = None,
                     scope: str = "default"):
  parsed_resources = await hse(loader.parse_resources(resources)
                               ) if resources else None
  parsed_fields = split(fields) if fields else None
  resources = await hse(
      loader.get_schema(parsed_resources, parsed_fields,
                        to_scope_mask({scope: True})))
  return ApiResponse(resources)


# TODO: add annotate+scope variable to get eg. the contract addresses for resources in response
@router.get("/last/{resources:path}")
@router.get("/last")
@limit(points=1)
async def get_last(req: Request,
                   resources: str,
                   quote: Optional[str] = None,
                   precision: int = 6):
  parsed_resources = await hse(loader.parse_resources(resources))
  last_values = await hse(
      loader.get_last_values(parsed_resources, quote, precision))
  return ApiResponse(last_values if len(parsed_resources) >
                     1 else last_values[parsed_resources[0]])


@router.get("/history/{resources:path}")
@router.get("/history")
@limit(points=10)
async def get_history(
    req: Request,
    resources: str,
    fields: str = "",
    from_date=None,
    to_date=None,
    interval: Optional[Interval] = None,
    target_epochs: Optional[int] = None,
    precision: int = 6,
    quote: Optional[str] = None,
    format: DataFormat = "json:row"  # json default to json:row
):
  parsed_resources, parsed_fields = await hse(
      loader.parse_resources_fields(resources, fields))
  if parsed_resources is None:
    return ApiResponse(parsed_resources, data_format=format)
  from_date, to_date, interval, target_epochs = fit_date_params(
      from_date, to_date, interval, target_epochs)
  data = await hse(
      loader.get_history(parsed_resources, parsed_fields, from_date, to_date,
                         interval, quote, precision, format))
  return ApiResponse(data, data_format=format)


@router.get("/convert/{pair:path}")
@router.get("/convert")
@limit(points=2)
async def get_convert(req: Request,
                      pair: str,
                      base_amount: Optional[float] = None,
                      quote_amount: Optional[float] = None,
                      precision: int = 6):
  result = await hse(
      converter.convert(pair, base_amount, quote_amount, precision))
  return ApiResponse(result)


@router.get("/pegcheck/{pair:path}")
@router.get("/pegcheck")
@limit(points=1)
async def get_pegcheck(req: Request,
                       pair: str,
                       factor: float = 1.0,
                       max_deviation: float = .002,
                       precision: int = 6):
  result = await hse(converter.pegcheck(pair, factor, max_deviation,
                                        precision))
  return ApiResponse(result)


@router.get("/limits")
@limit(points=5)
async def get_limits(req: Request):
  requester_id = gatekeeeper.requester_id(req)
  user_limits = await hse(limiter.get_user_limits(requester_id))
  return ApiResponse(user_limits)


@router.get("/analysis/{resources:path}")
@router.get("/analysis")
@limit(points=10)
async def get_analysis(req: Request,
                       resources: str,
                       fields: str = "",
                       from_date=None,
                       to_date=None,
                       periods: str = "20",
                       precision: int = 6,
                       quote: Optional[str] = None,
                       format: DataFormat = "json:row"):
  parsed_resources, parsed_fields = await hse(
      loader.parse_resources_fields(resources, fields))
  if parsed_resources is None:
    return ApiResponse(parsed_resources, data_format=format)
  parsed_periods = [int(p) for p in split(periods)]

  return ApiResponse(await hse(
      ts_analysis.get_all(
          parsed_resources,
          parsed_fields,
          from_date,
          to_date,
          "m5",  # default interval
          parsed_periods,
          quote,
          precision,
          None,  # no DataFrame provided
          format)))


@router.get("/volatility/{resources:path}")
@router.get("/volatility")
@limit(points=5)
async def get_volatility(req: Request,
                         resources: str,
                         fields: str = "idx",
                         from_date=None,
                         to_date=None,
                         periods: str = "20",
                         precision: int = 6,
                         quote: Optional[str] = None,
                         format: DataFormat = "json:row"):
  parsed_resources = await hse(loader.parse_resources(resources))
  parsed_fields = await hse(loader.parse_fields(parsed_resources[0], fields))
  parsed_periods = [int(p) for p in split(periods)]

  return ApiResponse(await hse(
      ts_analysis.get_volatility(
          parsed_resources,
          parsed_fields,
          from_date,
          to_date,
          "m5",  # default interval
          parsed_periods,
          quote,
          precision,
          format)))


@router.get("/trend/{resources:path}")
@router.get("/trend")
@limit(points=5)
async def get_trend(req: Request,
                    resources: str,
                    fields: str = "",
                    from_date=None,
                    to_date=None,
                    periods: str = "20",
                    precision: int = 6,
                    quote: Optional[str] = None,
                    format: DataFormat = "json:row"):
  parsed_resources = await hse(loader.parse_resources(resources))
  parsed_fields = await hse(loader.parse_fields(parsed_resources[0], fields))
  parsed_periods = [int(p) for p in split(periods)]

  return ApiResponse(await hse(
      ts_analysis.get_trend(
          parsed_resources,
          parsed_fields,
          from_date,
          to_date,
          "m5",  # default interval
          parsed_periods,
          quote,
          precision,
          format)))


@router.get("/momentum/{resources:path}")
@router.get("/momentum")
@limit(points=5)
async def get_momentum(req: Request,
                       resources: str,
                       fields: str = "",
                       from_date=None,
                       to_date=None,
                       periods: str = "20",
                       precision: int = 6,
                       quote: Optional[str] = None,
                       format: DataFormat = "json:row"):
  parsed_resources = await hse(loader.parse_resources(resources))
  parsed_fields = await hse(loader.parse_fields(parsed_resources[0], fields))
  parsed_periods = [int(p) for p in split(periods)]

  return ApiResponse(await hse(
      ts_analysis.get_momentum(
          parsed_resources,
          parsed_fields,
          from_date,
          to_date,
          "m5",  # default interval
          parsed_periods,
          quote,
          precision,
          format)))


@router.get("/oprange/{resources:path}")
@router.get("/oprange")
@limit(points=1)
async def get_oprange(req: Request,
                      resources: str,
                      fields: str = "",
                      from_date=None,
                      to_date=None,
                      precision: int = 6,
                      quote: Optional[str] = None,
                      format: DataFormat = "json:row"):
  parsed_resources = await hse(loader.parse_resources(resources))
  parsed_fields = await hse(loader.parse_fields(parsed_resources[0], fields))

  return ApiResponse(await hse(
      ts_analysis.get_oprange(parsed_resources, parsed_fields, from_date,
                              to_date, precision, quote, format)))
