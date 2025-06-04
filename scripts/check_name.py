#!/usr/bin/env python3
"""
Check Naming Conventions Script - Validates Git branch and commit message formats

Validates Git branch and commit message formats according to BTR Supply Backend
contributing guidelines. Used in Git hooks to enforce development workflow standards.

Branch format: feat/, fix/, refac/, ops/, docs/ + description
Commit format: [feat], [fix], [refac], [ops], [docs] + capitalized message

Usage:
  python scripts/check_name.py -b  # Check branch name
  python scripts/check_name.py -c  # Check commit message
  python scripts/check_name.py -p  # Check pre-push (branch + commits)
"""

import re
import sys
import os
import subprocess

# Naming conventions from chomp/CONTRIBUTING.md
BRANCH_RE = re.compile(r'^(feat|fix|refac|ops|docs)/')
COMMIT_RE = re.compile(r'^\[(feat|fix|refac|ops|docs)\] [A-Z]')  # Must be capitalized

is_invalid = False


def run_cmd(cmd):
  """Run shell command silently and return output."""
  return subprocess.run(cmd,
                        shell=True,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.DEVNULL,
                        text=True).stdout.strip()


def record_failure(check_type, value):
  """Print error message and set global invalid flag."""
  global is_invalid
  print(f'[POLICY] Invalid {check_type}: {value.splitlines()[0]}',
        file=sys.stderr)
  is_invalid = True


# Parse command line arguments
script_args = sys.argv[1:]
check_branch = '-b' in script_args or '--check-branch' in script_args
check_commit = '-c' in script_args or '--check-commit' in script_args
check_push = '-p' in script_args or '--check-push' in script_args

# Get git repository root
project_root = run_cmd('git rev-parse --show-toplevel')
if not project_root:
  print("[POLICY] Error: Not a git repository?", file=sys.stderr)
  sys.exit(1)

# Determine commit message file path
commit_msg_file = None
if '--commit-msg-file' in script_args:
  try:
    commit_msg_file = script_args[script_args.index('--commit-msg-file') + 1]
  except IndexError:
    print("[POLICY] Error: --commit-msg-file flag requires an argument.",
          file=sys.stderr)
    is_invalid = True
elif check_commit:
  commit_msg_file = os.path.join(project_root, '.git', 'COMMIT_EDITMSG')

# Get current branch and check if it's protected
current_branch = run_cmd('git rev-parse --abbrev-ref HEAD')
is_protected_branch = current_branch in ('main', 'dev', 'HEAD')

# 1. Check Branch Name (if checking branch or push, and not protected)
if (check_branch or check_push) and not is_protected_branch:
  if not BRANCH_RE.match(current_branch):
    record_failure('branch name', current_branch)
    print("[POLICY] Branch names must start with: feat/, fix/, refac/, ops/, docs/", 
          file=sys.stderr)

# 2. Check Commit Message (if checking commit)
if check_commit:
  if commit_msg_file and os.path.exists(commit_msg_file):
    try:
      with open(commit_msg_file, 'r', encoding='utf-8') as f:
        commit_msg = f.read().strip()
      # Only fail if the message is not empty and doesn't match the pattern
      if commit_msg and not COMMIT_RE.match(commit_msg):
        record_failure('commit message format', commit_msg)
        print("[POLICY] Commit messages must start with: [feat], [fix], [refac], [ops], [docs] + capitalized message", 
              file=sys.stderr)
    except Exception as e:
      print(f"[POLICY] Error reading {commit_msg_file}: {e}", file=sys.stderr)
      is_invalid = True

# 3. Check Pre-push Commit Format (if checking push, and not protected)
if check_push and not is_protected_branch:
  # Determine commit range (try upstream, fallback to HEAD~1)
  upstream_ref = run_cmd(
      "git rev-parse --abbrev-ref --symbolic-full-name '@{u}'") or 'HEAD~1'
  # Get log of commit messages in the range
  commit_log = run_cmd(f"git log {upstream_ref}..HEAD --pretty=%B")
  # Validate each non-empty commit message
  for i, commit_text in enumerate(
      [msg for msg in commit_log.split('\n\n\n') if msg.strip()]):
    if not COMMIT_RE.match(commit_text):
      record_failure(f'pushed commit #{i+1} format', commit_text)
      print("[POLICY] All pushed commit messages must follow format: [type] Capitalized message", 
            file=sys.stderr)

if is_invalid:
  print('[POLICY] Failed - See chomp/CONTRIBUTING.md for naming conventions', 
        file=sys.stderr)
  sys.exit(1)

print('[POLICY] Passed', file=sys.stderr)
sys.exit(0) 