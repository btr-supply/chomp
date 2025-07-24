"""
Instance UID and name management for Chomp.
Provides unique identification for running instances.
"""

import random
import secrets
from os import environ as env
from pathlib import Path
from typing import Dict, List, Optional, Union

from .runtime import runtime
from .format import log_debug, log_warn, log_info, log_error


def get_instance_uid() -> str:
  """Get the current instance UID."""
  return runtime.get_uid()


def generate_instance_uid() -> str:
  """Generate a new instance UID."""
  uid = secrets.token_hex(16)
  runtime.set_uid(uid)
  return uid


def get_instance_name() -> Optional[str]:
  """Get the current instance name."""
  return runtime.get_instance_name()


def set_instance_name(name: str) -> None:
  """Set the instance name."""
  runtime.set_instance_name(name)


def get_or_generate_instance_name() -> str:
  """Get instance name, generating one if needed (synchronous version)."""
  name = runtime.get_instance_name()
  if not name:
    # Try to use sophisticated generation, but fall back to simple if needed
    try:
      import asyncio
      # Try to run the async version
      name = asyncio.run(generate_instance_name())
    except Exception:
      # Fallback to simple generation
      uid = runtime.get_uid()
      name = f"instance-{uid[:8]}"
    runtime.set_instance_name(name)
  return name


async def get_or_generate_instance_name_async() -> str:
  """Get instance name, generating one if needed (async version)."""
  name = runtime.get_instance_name()
  if not name:
    name = await generate_instance_name()
    runtime.set_instance_name(name)
  return name


def get_instance_info() -> dict:
  """Get complete instance information."""
  return runtime.get_instance_info()


def get_masked_uid(uid_masks_file: str = "uid-masks") -> str:
  """
    Get a masked version of the instance UID.

    Args:
        uid_masks_file: Path to the UID masks file

    Returns:
        Masked UID string
    """
  uid = get_instance_uid()

  # Try to find the masks file in various locations
  masks_paths: List[Path] = [
      Path(uid_masks_file),
      Path.cwd() / uid_masks_file,
      Path(__file__).parent.parent.parent / uid_masks_file
  ]

  masks_file = None
  for path in masks_paths:
    if path.exists():
      masks_file = path
      break

  if not masks_file:
    # If no masks file found, return first 8 characters
    return uid[:8]

  try:
    with open(masks_file, 'r') as f:
      masks = [line.strip() for line in f if line.strip()]

    if not masks:
      return uid[:8]

    # Use UID to select a mask deterministically
    mask_index = int(uid[:8], 16) % len(masks)
    return masks[mask_index]

  except (IOError, ValueError):
    # Fallback to truncated UID if masks file can't be read
    return uid[:8]


def load_uid_masks() -> Optional[Dict[str, Union[str, List[str]]]]:
  """Load names from uid-masks file"""
  try:
    uid_masks_file = env.get("UID_MASKS_FILE", "uid-masks")
    potential_paths = [
        Path(uid_masks_file),
        Path.cwd() / uid_masks_file,
        Path(__file__).parent.parent.parent / uid_masks_file
    ]

    for path in potential_paths:
      if path.exists():
        with open(path, "r", encoding="utf-8") as f:
          names = [line.strip() for line in f if line.strip()]
        log_debug(f"Loaded {len(names)} names from {path}")
        return {"names": names, "file": path.name}

    log_warn("UID masks file not found")
    return None
  except Exception as e:
    log_warn(f"Failed to load uid-masks: {e}")
    return None


async def generate_instance_name(uid: Optional[str] = None) -> str:
  """Generate instance name using two names with different starting letters"""
  try:
    names = load_uid_masks()
    if not names:
      if not uid:
        uid = get_instance_uid()
      log_warn(f"Using UID as instance name: {uid}")
      return uid

    # Separate by length
    short_names = [name for name in names["names"] if len(name) <= 5]
    if not short_names:
      short_names = names["names"]

    # Try to find two names with different starting letters
    max_attempts = 20
    for _ in range(max_attempts):
      first_name = random.choice(short_names)
      second_name = random.choice(names["names"])

      # Check different starting letters and at least one short name
      if (first_name[0].lower() != second_name[0].lower()
          and (len(first_name) <= 5 or len(second_name) <= 5)):
        break
    else:
      # Fallback: just ensure at least one short name
      first_name = random.choice(short_names)
      second_name = random.choice(names["names"])

    # Randomly order the names
    name_pair = [first_name, second_name]
    random.shuffle(name_pair)

    instance_name = f"{name_pair[0]}-{name_pair[1]}"
    log_info(f"Instance name: {instance_name}, UID: {uid}")
    return instance_name

  except Exception as e:
    log_error(f"Failed to generate instance name: {e}")
    return get_instance_uid()
