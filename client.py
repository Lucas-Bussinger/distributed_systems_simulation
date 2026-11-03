#!/usr/bin/env python3
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))
import client

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="CLI de controle e status do cluster (MC714)")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("status", help="Verifica o status de todos os nós")
    trig_parser = subparsers.add_parser("trigger", help="Dispara uma eleição manual em um nó")
    trig_parser.add_argument("--node", default="no_1:50051", help="Endereço do nó alvo")
    args = parser.parse_args()

    if args.command == "status":
        client.check_cluster_status()
    elif args.command == "trigger":
        client.trigger_manual_election(args.node)
    else:
        parser.print_help()
