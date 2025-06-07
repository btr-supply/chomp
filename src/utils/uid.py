"""
Purpose: Instance UID management and human-friendly naming
Generates unique identifiers and names for ingester instances
"""

import os
import socket
import hashlib
import sys
import random
from pathlib import Path
from typing import Optional
from os import environ as env

from .format import log_info, log_debug, log_error, log_warn

UID_FILE = ".uid"

# Roman numeral mapping for name suffixes
ROMAN_NUMERALS = [
    "",
    "I",
    "II",
    "III",
    "IV",
    "V",
    "VI",
    "VII",
    "VIII",
    "IX",
    "X",
    "XI",
    "XII",
    "XIII",
    "XIV",
    "XV",
    "XVI",
    "XVII",
    "XVIII",
    "XIX",
    "XX",
    "XXI",
    "XXII",
    "XXIII",
    "XXIV",
    "XXV",
    "XXVI",
    "XXVII",
    "XXVIII",
    "XXIX",
    "XXX",
]


def get_workdir_root() -> Path:
  """Get the root working directory for the current process"""
  workdir = env.get("WORKDIR", ".")
  return Path(workdir).resolve()


def generate_instance_uid() -> str:
  """Generate a unique instance identifier based on launch command, config string, and hostname"""
  try:
    hostname = socket.gethostname()
    launch_command = " ".join(sys.argv)

    # Get config string from environment or arguments
    config_string = env.get("INGESTER_CONFIGS", "")
    if not config_string and len(sys.argv) > 1:
      for i, arg in enumerate(sys.argv):
        if arg in ["-c", "--ingester_configs"] and i + 1 < len(sys.argv):
          config_string = sys.argv[i + 1]
          break

    # Create deterministic hash
    uid_source = f"{hostname}|{launch_command}|{config_string}"
    return hashlib.md5(uid_source.encode()).hexdigest()
  except Exception:
    # Fallback to basic hash
    fallback = f"{socket.gethostname()}-{os.getpid()}"
    return hashlib.md5(fallback.encode()).hexdigest()


def get_instance_uid() -> str:
  """Get the current instance UID (load from file or generate new)"""
  uid_file_path = get_workdir_root() / UID_FILE

  try:
    if uid_file_path.exists():
      with open(uid_file_path, "r") as f:
        uid = f.read().strip()
        if uid:
          log_debug(f"Loaded instance UID: {uid}")
          return uid
  except Exception as e:
    log_debug(f"Failed to read UID file: {e}")

  # Generate and save new UID
  uid = generate_instance_uid()
  try:
    with open(uid_file_path, "w") as f:
      f.write(uid)
    log_info(f"Generated and saved instance UID: {uid}")
  except Exception as e:
    log_debug(f"Failed to save UID file: {e}")

  return uid


def load_uid_masks() -> Optional[list[str]]:
  """Load the list of names from uid-masks file"""
  try:
    uid_masks_file = env.get("UID_MASKS_FILE", "uid-masks")

    # Try multiple potential locations
    potential_paths = [
        Path(uid_masks_file),
        Path.cwd() / uid_masks_file,
        Path(__file__).parent.parent.parent / uid_masks_file,
        Path(__file__).parent.parent.parent / "uid-masks",
    ]

    for path in potential_paths:
      if path.exists():
        with open(path, "r", encoding="utf-8") as f:
          names = [line.strip() for line in f if line.strip()]
        log_debug(f"Loaded {len(names)} names from uid-masks at {path}")
        return names

    log_warn(
        f"UID masks file not found at: {[str(p) for p in potential_paths]}")
    return None
  except Exception as e:
    log_warn(f"Failed to load uid-masks: {e}")
    return None


async def get_existing_instance_names(base_name: str) -> list[str]:
  """Get all existing instance names that start with base_name"""
  try:
    from .. import state

    if not hasattr(state, "tsdb") or not state.tsdb:
      return []

    await state.tsdb.ensure_connected()
    tables = await state.tsdb.list_tables()
    if "instances" not in tables:
      return []

    # Placeholder for actual TSDB-specific query implementation
    # For now, return empty list to allow basic functionality
    return []
  except Exception as e:
    log_debug(f"Failed to query existing instance names: {e}")
    return []


def find_next_suffix(existing_names: list[str], base_name: str) -> str:
  """Find the next available Roman numeral suffix"""
  if base_name not in existing_names:
    return base_name

  # Extract existing suffix indices
  existing_suffixes = []
  for name in existing_names:
    if name == base_name:
      existing_suffixes.append(0)
    elif name.startswith(f"{base_name}-"):
      suffix = name[len(base_name) + 1:]
      if suffix in ROMAN_NUMERALS:
        existing_suffixes.append(ROMAN_NUMERALS.index(suffix))

  # Find next available suffix
  next_index = max(existing_suffixes) + 1 if existing_suffixes else 1

  if next_index < len(ROMAN_NUMERALS):
    suffix = ROMAN_NUMERALS[next_index]
    return f"{base_name}-{suffix}" if suffix else base_name
  else:
    return f"{base_name}-{next_index}"


async def generate_instance_name() -> str:
  """Generate a unique human-friendly instance name"""
  try:
    names = load_uid_masks()

    # Fallback to UID if no masks available
    if not names:
      uid = get_instance_uid()
      log_warn(
          f"Using UID as instance name due to missing uid-masks file: {uid}")
      return uid

    # Try up to 10 random names
    for _ in range(10):
      base_name = random.choice(names)
      existing_names = await get_existing_instance_names(base_name)

      if not existing_names:
        return base_name

      unique_name = find_next_suffix(existing_names, base_name)
      if unique_name not in existing_names:
        return unique_name

    # Ultimate fallback
    import time

    return f"Instance-{int(time.time())}"

  except Exception as e:
    log_error(f"Failed to generate instance name: {e}")
    try:
      uid = get_instance_uid()
      log_warn(f"Using UID as instance name due to error: {uid}")
      return uid
    except Exception:
      import time

      return f"Instance-{int(time.time())}"
