import ast
import operator
import re
import numpy
import polars
from typing import Optional

from .decorators import cache as _cache

SAFE_TYPES = (int, float, str, list, set, map, tuple, dict, type
              )  # Basic types
SAFE_OPERATORS = [
    getattr(operator, op) for op in dir(operator)
    if not op.startswith('_') and op != 'not_'
]  # Math operators
SAFE_FUNCTIONS = {
    'numpy': [
        'sin', 'cos', 'tan', 'log', 'exp', 'sqrt', 'mean', 'median', 'std',
        'var', 'sum', 'cumsum', 'min', 'max', 'abs', 'round', 'floor', 'ceil',
        'clip', 'where', 'concatenate', 'stack', 'hstack', 'vstack', 'split',
        'array', 'zeros', 'ones', 'full', 'empty', 'arange', 'linspace',
        'logspace', 'eye', 'diag', 'tril', 'triu', 'identity', 'dot', 'matmul',
        'tensordot', 'einsum'
    ],
    'polars': [
        'DataFrame',
        'Series',
        # 'info', 'head', 'tail', 'describe', 'shape', 'columns', 'index', 'values', 'dtypes', 'astype',
        # 'copy', 'drop', 'dropna', 'fillna', 'interpolate', 'replace', 'apply', 'applymap', 'map', 'groupby',
        # 'agg', 'transform', 'pivot', 'pivot_table', 'melt', 'merge', 'join', 'concat', 'append', 'sort_values', 'sort_index',
        # 'set_index', 'reset_index', 'loc', 'iloc', 'at', 'iat', 'sum', 'mean', 'median', 'std', 'var',
        # 'count', 'nunique', 'unique', 'value_counts', 'isna', 'isnull', 'notna', 'notnull', 'eq', 'ne', 'lt', 'le', 'gt', 'ge'
    ]
}
BASE_NAMESPACE = {op.__name__: op for op in SAFE_OPERATORS}
BASE_NAMESPACE.update({
    func: getattr(module, func)
    for module in [numpy, polars]
    for func in SAFE_FUNCTIONS[module.__name__]
})
BASE_NAMESPACE.update({t.__name__: t for t in SAFE_TYPES})
# BASE_NAMESPACE.update({'numpy': numpy, 'pd': polars}) # Add numpy and pd modules themselves
SAFE_EXPR_CACHE = set()


@_cache(ttl=3600, maxsize=512)
def safe_eval(expr, lambda_check=False, callable_check=False, **kwargs):
  """
  Evaluate a string expression in a safe environment.

  :param expr: string expression to evaluate
  :param **kwargs: additional variables to inject into the evaluation namespace
  :return: result of the evaluation
  """

  if type(expr) is not str:
    if callable(expr):
      return expr
    raise ValueError("Expression must be a string")
  ns = BASE_NAMESPACE.copy()
  ns.update(kwargs)

  try:
    match = re.match(r'^\s*(def\s+\w+\s*\(|lambda\s+)', expr)
    should_be_func = match and match.group(1).startswith('def')
    should_be_lambda = match and match.group(1).startswith('lambda')

    tree = ast.parse(expr, mode='exec' if should_be_func else 'eval')

    # Improved boolean logic with proper type checking
    is_func: bool = bool(should_be_func and isinstance(tree, ast.Module)
                         and len(tree.body) > 0
                         and isinstance(tree.body[0], ast.FunctionDef))
    is_lambda: bool = bool(should_be_lambda
                           and isinstance(tree, ast.Expression)
                           and isinstance(tree.body, ast.Lambda))

    if lambda_check and not is_lambda:
      raise ValueError("Expression must be a lambda")
    if callable_check and not (is_func or is_lambda):
      raise ValueError("Expression must be callable")

    # basic AST safety analysis
    if expr not in SAFE_EXPR_CACHE and not is_ast_safe(tree):
      raise ValueError("Invalid or unsafe expression")

    # compile the AST and evaluate
    if is_func and isinstance(tree, ast.Module):
      code = compile(tree, filename='<ast>', mode='exec')
      func_name: Optional[str] = None
      if tree.body and isinstance(tree.body[0], ast.FunctionDef):
        func_name = tree.body[0].name
      exec(code, ns)
      result = ns[func_name] if func_name else None
    elif isinstance(tree, ast.Expression):
      code = compile(tree, filename='<ast>', mode='eval')
      result = eval(code, ns)
    else:
      raise ValueError("Invalid AST type for compilation")

    # cache the expression as safe for faster evals
    SAFE_EXPR_CACHE.add(expr)
    return result

  except Exception as e:
    raise ValueError("Error evaluating expression: {}".format(e))


def is_ast_safe(tree, allowed_functions=SAFE_FUNCTIONS):
  """
  Basic AST safety analysis with granular function allowance

  :param tree: AST node
  :param allowed_functions: dictionary of allowed functions per module
  :return: True if the AST is safe, False otherwise
  """
  # Handle different node types
  for node in ast.walk(tree):
    # Check for dangerous function calls
    if isinstance(node, ast.Call):
      if isinstance(node.func, ast.Name):
        func_name = node.func.id
        # Block dangerous built-in functions
        if func_name in [
            'exec', 'eval', '__import__', 'open', 'compile', 'globals',
            'locals', 'vars', 'dir', 'getattr', 'setattr', 'delattr', 'hasattr'
        ]:
          return False
      elif isinstance(node.func, ast.Attribute):
        # Block dangerous attribute access
        if isinstance(node.func.value, ast.Name):
          if node.func.value.id == '__builtins__':
            return False

    # Check for dangerous name access
    elif isinstance(node, ast.Name):
      name = node.id
      # Block access to dangerous names
      if name in [
          '__import__', '__builtins__', '__globals__', '__locals__', 'exec',
          'eval', 'open', 'compile'
      ]:
        return False

    # Check for dangerous attribute access
    elif isinstance(node, ast.Attribute):
      if isinstance(node.value, ast.Name):
        if node.value.id in ['__builtins__', '__class__', '__dict__']:
          return False
      # Block dunder attributes
      if node.attr.startswith('__') and node.attr.endswith('__'):
        return False

  return True


def safe_eval_to_lambda(expr, **kwargs):
  """
  Evaluate a string expression as a lambda function in a safe environment.

  :param expr: string expression to evaluate
  :param **kwargs: additional variables to inject into the evaluation namespace
  :return: lambda function
  """
  return lambda **inner_kwargs: safe_eval(expr, **{**kwargs, **inner_kwargs})
