from contextlib import contextmanager
import importlib
import os

@contextmanager
def add_to_path(p):
    import sys
    old_path = sys.path
    sys.path = sys.path[:]
    sys.path.insert(0, p)
    try:
        yield
    finally:
        sys.path = old_path

def path_import(absolute_path):
   '''implementation taken from https://docs.python.org/3/library/importlib.html#importing-a-source-file-directly'''
   with add_to_path(os.path.dirname(absolute_path)):
       spec = importlib.util.spec_from_file_location(absolute_path, absolute_path)
       module = importlib.util.module_from_spec(spec)
       spec.loader.exec_module(module)
       return module