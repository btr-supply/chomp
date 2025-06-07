# Core ingesters that don't require optional dependencies
from . import http_api
from . import ws_api
from . import processor
from . import monitor

# Optional ingesters that require specific dependencies
import importlib.util

# Static scrapper (requires beautifulsoup4, requests)
_has_static_scrapper = importlib.util.find_spec('.static_scrapper',
                                                package=__name__) is not None
if _has_static_scrapper:
  from . import static_scrapper  # noqa: F401

# EVM ingesters (requires web3)
_has_evm = (
    importlib.util.find_spec('.evm_caller', package=__name__) is not None
    and importlib.util.find_spec('.evm_logger', package=__name__) is not None)
if _has_evm:
  from . import evm_caller  # noqa: F401
  from . import evm_logger  # noqa: F401

# SVM ingesters (requires solders)
_has_svm = (
    importlib.util.find_spec('.svm_caller', package=__name__) is not None
    and importlib.util.find_spec('.svm_logger', package=__name__) is not None)
if _has_svm:
  from . import svm_caller  # noqa: F401
  from . import svm_logger  # noqa: F401

# Web3 misc ingesters (requires various web3 libraries)
_has_web3_misc = (
    importlib.util.find_spec('.sui_caller', package=__name__) is not None
    and importlib.util.find_spec('.sui_logger', package=__name__) is not None
    and importlib.util.find_spec('.aptos_logger', package=__name__) is not None
    and importlib.util.find_spec('.ton_caller', package=__name__) is not None
    and importlib.util.find_spec('.ton_logger', package=__name__) is not None)
if _has_web3_misc:
  from . import sui_caller  # noqa: F401
  from . import sui_logger  # noqa: F401
  from . import aptos_logger  # noqa: F401
  from . import ton_caller  # noqa: F401
  from . import ton_logger  # noqa: F401

# Export list for explicit imports
__all__ = ['http_api', 'ws_api', 'processor', 'monitor']

if _has_static_scrapper:
  __all__.append('static_scrapper')

if _has_evm:
  __all__.extend(['evm_caller', 'evm_logger'])

if _has_svm:
  __all__.extend(['svm_caller', 'svm_logger'])

if _has_web3_misc:
  __all__.extend(
      ['sui_caller', 'sui_logger', 'aptos_logger', 'ton_caller', 'ton_logger'])
