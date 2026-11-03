#!/usr/bin/env python3
"""
Redirecionador para src/generate_protos.py
"""
import sys
import os

base_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(base_dir, "src"))

from generate_protos import compile_proto

if __name__ == "__main__":
    sys.exit(compile_proto())
