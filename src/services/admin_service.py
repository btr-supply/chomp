"""
Admin service for configuration management and system control.
Uses diff-match-patch for efficient versioning and yamale for validation.
"""

import os
import tempfile
from datetime import datetime, timezone
from typing import Dict, List, Any
from pathlib import Path

from diff_match_patch import diff_match_patch

from ..model import ServiceResponse
from .. import state
from ..cache import cache, get_cache
from ..proxies import ConfigProxy

UTC = timezone.utc

# Configuration Management
CONFIG_HISTORY_KEY = "config:history"
CONFIG_HISTORY_LIMIT = 50


class ConfigVersionManager:
    """Manages configuration versioning using diff-match-patch for efficient storage"""

    def __init__(self):
        self.dmp = diff_match_patch()

    def _get_config_path(self) -> Path:
        """Get the configuration file path"""
        config_path = (
            state.args.config
            if hasattr(state.args, "config") and state.args.config
            else "chomp/ingesters.yml"
        )
        return Path(config_path).resolve()

    async def save_config_with_diff(self, new_content: str) -> str:
        """Save new config and store diff in Redis history"""
        try:
            config_path = self._get_config_path()

            # Read current config if it exists
            current_content = ""
            if config_path.exists():
                with open(config_path, "r") as f:
                    current_content = f.read()

            # Compute diff using diff-match-patch
            if current_content != new_content:
                patches = self.dmp.patch_make(current_content, new_content)
                diff_text = self.dmp.patch_toText(patches)

                # Store diff in Redis history
                if diff_text.strip():
                    await self._store_diff_in_history(diff_text)

            # Write the new config file
            with open(config_path, "w") as f:
                f.write(new_content)

            return "Config updated successfully. Diff stored in history."

        except Exception as e:
            raise Exception(f"Failed to save config with diff: {e}")

    async def _store_diff_in_history(self, diff_text: str) -> None:
        """Store diff in Redis with timestamp and limit history size"""
        timestamp = datetime.now(UTC).isoformat()
        diff_entry = {"timestamp": timestamp, "diff": diff_text}

        # Get current history
        history = await get_cache(CONFIG_HISTORY_KEY, pickled=True) or []

        # Add new diff to the beginning (most recent first)
        history.insert(0, diff_entry)

        # Limit history size
        if len(history) > CONFIG_HISTORY_LIMIT:
            history = history[:CONFIG_HISTORY_LIMIT]

        # Store back to Redis
        await cache(CONFIG_HISTORY_KEY, history, pickled=True)

    async def get_config_at_version(self, steps_back: int) -> str:
        """Reconstruct config at a specific version by applying patches backwards"""
        if steps_back < 0:
            raise ValueError("steps_back must be non-negative")

        config_path = self._get_config_path()

        if steps_back == 0:
            # Return current version
            if config_path.exists():
                with open(config_path, "r") as f:
                    return f.read()
            return ""

        # Get current config
        if not config_path.exists():
            raise FileNotFoundError("Current config file not found")

        with open(config_path, "r") as f:
            current_content = f.read()

        # Get history from Redis
        history = await get_cache(CONFIG_HISTORY_KEY, pickled=True) or []

        if steps_back > len(history):
            raise ValueError(
                f"Cannot go back {steps_back} steps, only {len(history)} diffs available"
            )

        # Apply patches backwards
        content = current_content
        for i in range(steps_back):
            diff_entry = history[i]
            diff_text = diff_entry["diff"]

            # Use diff-match-patch for reverse patching
            patches = self.dmp.patch_fromText(diff_text)
            # For reverse patching, we need to invert the patches
            for patch in patches:
                # Invert each patch's diffs
                inverted_diffs = []
                for diff_op, diff_text in patch.diffs:
                    if diff_op == 1:  # INSERT becomes DELETE
                        inverted_diffs.append((-1, diff_text))
                    elif diff_op == -1:  # DELETE becomes INSERT
                        inverted_diffs.append((1, diff_text))
                    else:  # EQUAL stays the same
                        inverted_diffs.append((diff_op, diff_text))
                patch.diffs = inverted_diffs

            content, _ = self.dmp.patch_apply(patches, content)

        return content

    async def get_history_list(self) -> List[Dict[str, Any]]:
        """Get list of available config versions with timestamps"""
        history = await get_cache(CONFIG_HISTORY_KEY, pickled=True) or []
        return [
            {"version": i + 1, "timestamp": entry["timestamp"], "steps_back": i + 1}
            for i, entry in enumerate(history)
        ]

    async def rollback_to_version(self, steps_back: int) -> str:
        """Rollback config to a specific version"""
        try:
            # Get the config content at the specified version
            rolled_back_content = await self.get_config_at_version(steps_back)

            # Validate the rolled back content
            err, validation = await validate_config(rolled_back_content)
            if err or not validation["valid"]:
                raise ValueError("Rolled back configuration is invalid")

            # Save the rolled back content (this will create a new diff entry)
            await self.save_config_with_diff(rolled_back_content)

            return f"Successfully rolled back {steps_back} steps"

        except Exception as e:
            raise Exception(f"Rollback failed: {e}")


config_manager = ConfigVersionManager()


async def get_current_config() -> ServiceResponse[str]:
    """Get current configuration as YAML string."""
    try:
        config_path = config_manager._get_config_path()

        if not config_path.exists():
            return f"Configuration file not found: {config_path}", ""

        with open(config_path, "r", encoding="utf-8") as f:
            return "", f.read()
    except Exception as e:
        return f"Failed to read configuration: {e}", ""


async def validate_config(config_content: str) -> ServiceResponse[Dict[str, Any]]:
    """Validate configuration using yamale validation."""
    try:
        # Write content to temp file for validation
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as tmp:
            tmp.write(config_content)
            tmp_path = tmp.name

        try:
            # Use existing ConfigProxy validation
            config = ConfigProxy.load_config(tmp_path)
            affected_ingesters = [ing.name for ing in config.ingesters]

            return "", {
                "valid": True,
                "errors": [],
                "warnings": [],
                "affected_ingesters": affected_ingesters,
            }
        except Exception as e:
            return "", {
                "valid": False,
                "errors": [str(e)],
                "warnings": [],
                "affected_ingesters": [],
            }
        finally:
            os.unlink(tmp_path)

    except Exception as e:
        return f"Validation failed: {e}", {}


async def update_config(config_content: str) -> ServiceResponse[Dict[str, Any]]:
    """Update configuration with diff-match-patch versioning."""
    try:
        # Validate first
        err, validation_result = await validate_config(config_content)
        if err or not validation_result["valid"]:
            return err or "Configuration validation failed", {
                "success": False,
                "errors": validation_result.get("errors", []),
            }

        # Use ConfigVersionManager for diff-based storage
        result_message = await config_manager.save_config_with_diff(config_content)

        return "", {
            "success": True,
            "message": result_message,
            "affected_ingesters": validation_result["affected_ingesters"],
        }
    except Exception as e:
        return f"Configuration update failed: {e}", {}


async def get_config_history(limit: int = 10) -> ServiceResponse[List[Dict[str, Any]]]:
    """Get configuration version history."""
    try:
        history = await config_manager.get_history_list()
        return "", history[:limit]
    except Exception as e:
        return f"Failed to get config history: {e}", []


async def rollback_config(steps_back: int) -> ServiceResponse[Dict[str, Any]]:
    """Rollback to previous configuration version."""
    try:
        result_message = await config_manager.rollback_to_version(steps_back)

        # Get affected ingesters from current config
        err, current_config = await get_current_config()
        if not err:
            _, validation = await validate_config(current_config)
            affected_ingesters = validation.get("affected_ingesters", [])
        else:
            affected_ingesters = []

        return "", {
            "success": True,
            "message": result_message,
            "affected_ingesters": affected_ingesters,
        }
    except Exception as e:
        return f"Rollback failed: {e}", {}
