#!/usr/bin/env python3
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))
import run_2pc_experiments

if __name__ == "__main__":
    run_2pc_experiments.main()

