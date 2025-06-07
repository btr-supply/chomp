#!/usr/bin/env python3
"""
Test script to validate adapter implementations syntax and basic structure.
This script mocks external dependencies to test the adapters without requiring
actual database installations.
"""

import sys
import ast


def test_adapter_syntax(adapter_path: str) -> tuple[bool, str]:
  """Test if an adapter file has valid Python syntax."""
  try:
    with open(adapter_path, 'r') as f:
      content = f.read()

    # Parse the AST to check syntax
    ast.parse(content)
    return True, "OK"
  except SyntaxError as e:
    return False, f"Syntax Error: {e}"
  except Exception as e:
    return False, f"Error: {e}"


def test_adapter_structure(adapter_path: str,
                           expected_class: str) -> tuple[bool, str]:
  """Test if an adapter has the expected class structure."""
  try:
    with open(adapter_path, 'r') as f:
      content = f.read()

    tree = ast.parse(content)

    # Find the main adapter class
    classes = [
        node for node in ast.walk(tree) if isinstance(node, ast.ClassDef)
    ]
    adapter_class = None

    for cls in classes:
      if cls.name == expected_class:
        adapter_class = cls
        break

    if not adapter_class:
      return False, f"Class {expected_class} not found"

    # Check required methods
    required_methods = [
        "connect", "ping", "ensure_connected", "close", "create_db", "use_db",
        "create_table", "insert", "insert_many", "fetch", "fetchall",
        "fetch_batch", "commit", "list_tables"
    ]

    methods = [
        node.name for node in adapter_class.body
        if isinstance(node, ast.FunctionDef)
    ]
    missing_methods = [
        method for method in required_methods if method not in methods
    ]

    if missing_methods:
      return False, f"Missing methods: {missing_methods}"

    return True, "All required methods present"

  except Exception as e:
    return False, f"Error analyzing structure: {e}"


def main():
  """Test all implemented adapters."""
  adapters = [
      ("chomp/src/adapters/opentsdb.py", "OpenTsdb"),
      ("chomp/src/adapters/timescale.py", "TimescaleDb"),
      ("chomp/src/adapters/questdb.py", "QuestDb"),
      ("chomp/src/adapters/victoriametrics.py", "VictoriaMetrics"),
  ]

  print("🔍 Testing Time Series Database Adapters")
  print("=" * 50)

  all_passed = True

  for adapter_path, expected_class in adapters:
    print(f"\n📁 Testing {adapter_path}")

    # Test syntax
    syntax_ok, syntax_msg = test_adapter_syntax(adapter_path)
    if syntax_ok:
      print(f"   ✅ Syntax: {syntax_msg}")
    else:
      print(f"   ❌ Syntax: {syntax_msg}")
      all_passed = False
      continue

    # Test structure
    structure_ok, structure_msg = test_adapter_structure(
        adapter_path, expected_class)
    if structure_ok:
      print(f"   ✅ Structure: {structure_msg}")
    else:
      print(f"   ❌ Structure: {structure_msg}")
      all_passed = False

  print("\n" + "=" * 50)
  if all_passed:
    print("🎉 All adapters passed validation!")
  else:
    print("❌ Some adapters failed validation.")

  return 0 if all_passed else 1


if __name__ == "__main__":
  sys.exit(main())
