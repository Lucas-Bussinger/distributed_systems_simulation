#!/usr/bin/env python3
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))
import test_2pc_scenarios

if __name__ == "__main__":
    test_2pc_scenarios.run_all_tests()

