from typing import Optional
from ..utils import round_sigfig, split
from ..model import ServiceResponse
from . import loader

async def convert(pair: str, base_amount: Optional[float] = None, quote_amount: Optional[float] = None, precision: int = 6) -> ServiceResponse[dict]:
  """Convert between two resources using their last values"""
  # Validate pair format
  if not ('-' in pair and '.' in pair):
    return "Invalid pair format. Must be 'resource.field-resource.field'", {}
  if base_amount and quote_amount:
    return "Cannot specify both base and quote amounts", {}

  try:
    base, quote = split(pair)
    base_resource, base_field = base.split('.', 1)
    quote_resource, quote_field = quote.split('.', 1)
  except ValueError:
    return "Both base and quote must contain a field specified with dot notation", {}

  # Fetch last values for resources
  err, lasts = await loader.get_last_values([base_resource, quote_resource])
  if err:
    return err, {}

  # Extract and validate the fields
  try:
    base_value = round_sigfig(lasts[base_resource][base_field], precision)
    quote_value = round_sigfig(lasts[quote_resource][quote_field], precision)
  except (KeyError, TypeError):
    return "Required fields not found in resources", {}

  # Calculate conversion
  rate = round_sigfig(base_value / quote_value, precision)
  if quote_amount:
    result = quote_amount / rate
    base_amount = result
  else:
    base_amount = base_amount or 1
    result = base_amount * rate
    quote_amount = result

  return "", {
    'base': base,
    'quote': quote,
    'base_amount': base_amount,
    'quote_amount': quote_amount,
    'rate': rate,
    'result': result,
    'precision': precision
  }

async def pegcheck(pair: str, factor: float = 1.0, max_deviation: float = .002, precision: int = 6) -> ServiceResponse[dict]:
  """Check if two resources are pegged within a specified deviation"""
  try:
    base, quote = pair.split('-')
    base_resource, base_field = base.split('.')
    quote_resource, quote_field = quote.split('.')
  except ValueError:
    return "Both base and quote must contain a field specified with dot notation", {}

  # Get last values for both resources
  err, lasts = await loader.get_last_values([base_resource, quote_resource], precision=precision)
  if err:
    return err, {}

  # Extract prices
  try:
    base_price = lasts[base_resource][base_field]
    quote_price = lasts[quote_resource][quote_field]
  except (KeyError, TypeError):
    return "Required fields not found in resources", {}

  # Calculate adjusted base price
  adjusted_base = round_sigfig(base_price * factor, precision)
  min_quote = round_sigfig(quote_price * (1 - max_deviation), precision)
  max_quote = round_sigfig(quote_price * (1 + max_deviation), precision)
  deviation = round((adjusted_base - quote_price) / quote_price, 4)

  return "", {
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
