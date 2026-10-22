#!/usr/bin/env python3
"""
CLI de controle e inspeção do cluster de nós (Algoritmo do Anel).
Suporta 10 nós com IDs aleatórios.
"""

import sys
import os
import argparse
import grpc

try:
    import election_pb2
    import election_pb2_grpc
    if not hasattr(election_pb2, "NodeInfo"):
        raise ImportError("Proto desatualizado")
except (ImportError, AttributeError):
    from generate_protos import compile_proto
    compile_proto()
    import election_pb2
    import election_pb2_grpc

DEFAULT_NODES = [
    ("no_1", "no_1:50051"),
    ("no_2", "no_2:50051"),
    ("no_3", "no_3:50051"),
    ("no_4", "no_4:50051"),
    ("no_5", "no_5:50051"),
    ("no_6", "no_6:50051"),
    ("no_7", "no_7:50051"),
    ("no_8", "no_8:50051"),
    ("no_9", "no_9:50051"),
    ("no_10", "no_10:50051"),
]

def check_cluster_status(nodes=DEFAULT_NODES):
    print("=" * 95)
    print(f"{'CONTAINER':<10} | {'ID (ALEATÓRIO)':<15} | {'ENDEREÇO':<14} | {'STATUS':<11} | {'LÍDER ELEITO':<16} | {'PAPEL':<10} | {'ELEIÇÃO?'}")
    print("=" * 95)

    for cname, addr in nodes:
        try:
            with grpc.insecure_channel(addr) as channel:
                stub = election_pb2_grpc.RingServiceStub(channel)
                res = stub.GetStatus(election_pb2.StatusRequest(), timeout=1.5)
                role = "👑 LÍDER" if res.is_leader else "FOLLOWER"
                leader_str = f"Nó {res.current_leader}" if res.current_leader > 0 else "Nenhum"
                elec_str = "Sim" if res.election_in_progress else "Não"
                print(f"{cname:<10} | {res.node_id:<15} | {addr:<14} | {'🟢 ONLINE':<11} | {leader_str:<16} | {role:<10} | {elec_str}")
        except Exception:
            print(f"{cname:<10} | {'-':<15} | {addr:<14} | {'🔴 OFFLINE':<11} | {'-':<16} | {'-':<10} | {'-'}")
    print("=" * 95)

def trigger_election_on_node(addr: str, reason: str = "cli_manual_trigger"):
    print(f"Disparando eleição no nó em {addr}...")
    try:
        with grpc.insecure_channel(addr) as channel:
            stub = election_pb2_grpc.RingServiceStub(channel)
            res = stub.TriggerElection(election_pb2.TriggerRequest(reason=reason), timeout=2.5)
            print(f"Resposta: started={res.started}, mensagem='{res.message}'")
    except Exception as e:
        print(f"Erro ao disparar eleição em {addr}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Gerenciador CLI do Algoritmo do Anel (10 nós)")
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("status", help="Exibe o status de todos os 10 nós do anel")

    trig_parser = subparsers.add_parser("trigger", help="Dispara uma eleição em um nó específico")
    trig_parser.add_argument("--node", default="no_1:50051", help="Endereço do nó (ex: no_1:50051)")
    trig_parser.add_argument("--reason", default="manual_test", help="Motivo da eleição")

    args = parser.parse_args()

    if args.command == "status" or not args.command:
        check_cluster_status()
    elif args.command == "trigger":
        trigger_election_on_node(args.node, args.reason)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
