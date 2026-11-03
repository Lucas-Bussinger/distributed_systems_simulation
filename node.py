#!/usr/bin/env python3
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))
from node import DistributedBankNode

if __name__ == "__main__":
    node = DistributedBankNode()
    node.serve()
