# Lazy imports - adapters are loaded on demand to avoid import errors for missing dependencies
# Import only basic modules that don't have optional dependencies

# Base classes and utilities that don't require optional dependencies
from .jsonrpc import JsonRpcClient
from .evm_rpc import EvmRpcClient
from .sui_rpc import SuiRpcClient
from .svm_rpc import SvmRpcClient


# Database adapters with optional dependencies are imported lazily
# Use get_adapter() function to safely import them
def get_adapter(adapter_name: str):
  """Lazily import database adapters to avoid import errors for missing dependencies"""
  from ..utils.deps import safe_import

  adapter_mapping = {
      "tdengine": ("src.adapters.tdengine", "Taos"),
      "sqlite": ("src.adapters.sqlite", "SQLite"),
      "clickhouse": ("src.adapters.clickhouse", "ClickHouse"),
      "duckdb": ("src.adapters.duckdb", "DuckDB"),
      "timescale": ("src.adapters.timescale", "TimescaleDb"),
      "questdb": ("src.adapters.questdb", "QuestDb"),
      "mongodb": ("src.adapters.mongodb", "MongoDb"),
      "influxdb": ("src.adapters.influxdb", "InfluxDb"),
      "victoriametrics": ("src.adapters.victoriametrics", "VictoriaMetrics"),
      "kx": ("src.adapters.kx", "Kx"),
  }

  if adapter_name.lower() in adapter_mapping:
    module_path, class_name = adapter_mapping[adapter_name.lower()]
    module = safe_import(module_path)
    if module and hasattr(module, class_name):
      return getattr(module, class_name)

  return None


__all__ = [
    "JsonRpcClient",
    "EvmRpcClient",
    "SuiRpcClient",
    "SvmRpcClient",
    "get_adapter",
]
