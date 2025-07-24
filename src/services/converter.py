from typing import Optional
from ..utils import round_sigfig, split, log_warn
from ..utils.decorators import service_method
from . import loader


@service_method("convert currency pair")
async def convert(pair: str,
                  base_amount: Optional[float] = None,
                  quote_amount: Optional[float] = None,
                  precision: int = 6) -> dict:
  """Convert between two resources using their last values"""
  # Validate pair format
  if not ('-' in pair and '.' in pair):
    log_warn(f"Invalid pair format provided: {pair}")
    raise ValueError(
        "Invalid pair format. Must be 'resource.field-resource.field'")
  if base_amount and quote_amount:
    log_warn("Both base and quote amounts specified in conversion")
    raise ValueError("Cannot specify both base and quote amounts")

  base, quote = split(pair)
  base_resource, base_field = base.split('.', 1)
  quote_resource, quote_field = quote.split('.', 1)

  # Fetch last values for resources
  lasts = await loader.get_last_values([base_resource, quote_resource])

  # Extract and validate the fields
  raw_base_value = lasts[base_resource][base_field]
  raw_quote_value = lasts[quote_resource][quote_field]

  # Validate that values are numeric
  try:
    base_value = round_sigfig(float(raw_base_value), precision)
    quote_value = round_sigfig(float(raw_quote_value), precision)
  except (ValueError, TypeError):
    raise ValueError("Field values must be numeric")

  # Calculate conversion
  if quote_value == 0:
    raise ValueError("Quote value cannot be zero")

  rate = round_sigfig(base_value / quote_value, precision)
  if quote_amount:
    result = quote_amount / rate
    base_amount = result
  else:
    base_amount = base_amount or 1
    result = base_amount * rate
    quote_amount = result

  return {
      'base': base,
      'quote': quote,
      'base_amount': base_amount,
      'quote_amount': quote_amount,
      'rate': rate,
      'result': result,
      'precision': precision
  }


@service_method("check currency peg")
async def pegcheck(pair: str,
                   factor: float = 1.0,
                   max_deviation: float = .002,
                   precision: int = 6) -> dict:
  """Check if two resources are pegged within a specified deviation"""
  base, quote = pair.split('-')
  base_resource, base_field = base.split('.')
  quote_resource, quote_field = quote.split('.')

  # Get last values for both resources
  lasts = await loader.get_last_values([base_resource, quote_resource],
                                       precision=precision)

  # Extract prices
  raw_base_price = lasts[base_resource][base_field]
  raw_quote_price = lasts[quote_resource][quote_field]

  # Validate that prices are numeric
  try:
    base_price = float(raw_base_price)
    quote_price = float(raw_quote_price)
  except (ValueError, TypeError):
    raise ValueError("Price values must be numeric")

  if quote_price == 0:
    raise ValueError("Quote price cannot be zero")

  # Calculate adjusted base price
  adjusted_base = round_sigfig(base_price * factor, precision)
  min_quote = round_sigfig(quote_price * (1 - max_deviation), precision)
  max_quote = round_sigfig(quote_price * (1 + max_deviation), precision)
  deviation = round((adjusted_base - quote_price) / quote_price, 4)

  return {
      'base': base,
      'quote': quote,
      'base_price': base_price,
      'quote_price': quote_price,
      'adjusted_base': adjusted_base,
      'factor': factor,
      'min_quote': min_quote,
      'max_quote': max_quote,
      'max_deviation': max_deviation,
      'deviation': deviation,
      'in_range': abs(deviation) <= max_deviation,
      'precision': precision
  }
