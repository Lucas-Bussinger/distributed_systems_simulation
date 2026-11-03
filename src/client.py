#!/usr/bin/env python3
"""
CLI de controle e inspeção do cluster de nós (Algoritmo do Anel e 2PC).
"""

import sys
import os
import argparse
import grpc

base_dir = os.path.dirname(os.path.abspath(__file__))
if base_dir not in sys.path:
    sys.path.insert(0, base_dir)

try:
    import election_pb2
    import election_pb2_grpc
    import bank_2pc_pb2
    import bank_2pc_pb2_grpc
except ImportError:
    from generate_protos import compile_proto
    compile_proto()
    import election_pb2
    import election_pb2_grpc
    import bank_2pc_pb2
    import bank_2pc_pb2_grpc

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
    print("=" * 105)
    print(f"{'CONTAINER':<10} | {'ID (ALEATÓRIO)':<15} | {'ENDEREÇO':<14} | {'STATUS':<11} | {'LÍDER ELEITO':<16} | {'PAPEL':<12} | {'ELEIÇÃO?'}")
    print("=" * 105)

    for cname, addr in nodes:
        try:
            with grpc.insecure_channel(addr) as channel:
                stub = election_pb2_grpc.RingServiceStub(channel)
                res = stub.GetStatus(election_pb2.StatusRequest(), timeout=1.5)
                role = "👑 LÍDER (2PC)" if res.is_leader else "FOLLOWER"
                leader_str = f"Nó {res.current_leader}" if res.current_leader > 0 else "Nenhum"
                elec_str = "Sim" if res.election_in_progress else "Não"
                print(f"{cname:<10} | {res.node_id:<15} | {addr:<14} | {'🟢 ONLINE':<11} | {leader_str:<16} | {role:<12} | {elec_str}")
        except Exception:
            print(f"{cname:<10} | {'-':<15} | {addr:<14} | {'🔴 OFFLINE':<11} | {'-':<16} | {'-':<12} | -")
    print("=" * 105)

def trigger_manual_election(target_addr: str):
    print(f"Disparando eleição manual no nó {target_addr}...")
    try:
        with grpc.insecure_channel(target_addr) as channel:
            stub = election_pb2_grpc.RingServiceStub(channel)
            res = stub.TriggerElection(election_pb2.TriggerRequest(reason="cli_trigger"), timeout=3.0)
            print(f"Resposta: started={res.started}, message='{res.message}'")
    except Exception as e:
        print(f"Erro ao disparar eleição: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CLI de controle e status do cluster (MC714)")
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("status", help="Verifica o status de todos os nós")

    trig_parser = subparsers.add_parser("trigger", help="Dispara uma eleição manual em um nó")
    trig_parser.add_argument("--node", default="no_1:50051", help="Endereço do nó alvo")

    args = parser.parse_args()

    if args.command == "status":
        check_cluster_status()
    elif args.command == "trigger":
        trigger_manual_election(args.node)
    else:
        parser.print_help()

