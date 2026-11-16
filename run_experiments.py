#!/usr/bin/env python3
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))
from run_experiments import run_experiment_suite

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Automação experimental do Algoritmo do Anel")
    parser.add_argument("--node", default="no_1:50051", help="Endereço do nó alvo")
    parser.add_argument("--rounds", type=int, default=10, help="Número de rodadas para teste")
    parser.add_argument("--poll-interval", type=float, default=0.0005, help="Intervalo de amostragem em segundos (default: 0.0005 = 0.5 ms)")
    args = parser.parse_args()

    run_experiment_suite(target_node=args.node, rounds=args.rounds, poll_interval=args.poll_interval)
