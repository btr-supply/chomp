from argparse import _ArgumentGroup, ArgumentParser
from typing import Any, Optional
from os import environ as env
from dotenv import dotenv_values

from .types import is_bool
from .format import prettify

class ArgParser(ArgumentParser):

  info: dict[str, tuple] = {}
  origin: dict[str, str]
  parsed: Any = None

  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self.info = {}  # Internal map for storing argument info
    self.origin = {}
    self.parsed: Any = None

  def add_argument(self, *args, **kwargs):
    if kwargs.get("group"):
      group = kwargs.pop("group")
      action = group.add_argument(*args, **kwargs)
    else:
      action = super().add_argument(*args, **kwargs)
    if not action.type:
      if is_bool(action.const or action.default):
        action.type = bool
      elif action.default is not None:
        action.type = type(action.default)
      else:
        action.type = str  # Default to str when no type and no default
    self.info[action.dest] = (action.type, action.default) # arg type and default value
    return action

  def get_info(self, arg_name: str) -> Optional[tuple]:
    return self.info.get(arg_name)

  def parse_args(self, *args, **kwargs) -> Any:
    self.parsed = super().parse_args(*args, **kwargs)
    return self.parsed

  def argument_tuple_to_kwargs(self, arg_tuple: tuple) -> tuple[Any, dict[str, Any]]:
    names_tuple, arg_type, default, action, help_str = arg_tuple
    args = names_tuple
    kwargs = {
      "default": default,
      "help": help_str
    }
    if arg_type and not action:
      kwargs["type"] = arg_type
    if action:
      kwargs["action"] = action
    return args, kwargs

  def add_arguments(self, arguments: list[tuple], group: Optional[_ArgumentGroup]=None) -> None:
    for arg_tuple in arguments:
      args, kwargs = self.argument_tuple_to_kwargs(arg_tuple)
      if group:
        kwargs["group"] = group
      self.add_argument(*args, **kwargs)

  def add_group(self, name: str, arguments: list[tuple]) -> None:
    group = self.add_argument_group(name)
    self.add_arguments(group=group, arguments=arguments)

  def add_groups(self, groups: dict[str, list[tuple]]) -> None:
    for group_name, arguments in groups.items():
      self.add_group(group_name, arguments)

  def load_env(self, path: Optional[str]=None) -> Any:
    if not self.parsed:
      self.parse_args()
    env_file = dotenv_values(path or self.parsed.env)
    # Filter out None values before updating env
    filtered_env = {k: v for k, v in env_file.items() if v is not None}
    env.update(filtered_env)
    for k, v in {**env_file, **vars(self.parsed)}.items():
      k_lower = k.lower()
      arg_type, arg_default = self.get_info(k_lower) or (type(v), None)
      # is_default = arg_default == v
      dotenv_val = env_file.get(k)
      env_os_val = env.get(k)
      if dotenv_val:
        selected = arg_type(dotenv_val) if not isinstance(arg_type, type) or arg_type is not bool else dotenv_val.lower() == "true"
        self.origin[k_lower] = ".env file"
      elif env_os_val:
        selected = arg_type(env_os_val) if not isinstance(arg_type, type) or arg_type is not bool else env_os_val.lower() == "true"
        self.origin[k_lower] = "os env"
      else:
        selected = v
        env[k.upper()] = str(v) # inject into env for naive access
        self.origin[k_lower] = "cli" # if is_default else "cli"
      setattr(self.parsed, k_lower, selected)
    return self.parsed

  def pretty(self) -> str:
    rows = [
      [arg, getattr(self.parsed, arg), self.origin.get(arg, "unknown")]
      for arg in vars(self.parsed)
    ]
    return prettify(data=rows, headers=["Name", "Value", "Source"])
