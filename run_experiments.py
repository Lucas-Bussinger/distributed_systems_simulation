#!/usr/bin/env python3
"""
Script de automação experimental para avaliação do Algoritmo do Anel (MC714).
Avalia o anel com 10 nós e IDs aleatórios.
"""

import sys
import time
import math
import argparse
from typing import List
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

def calculate_percentiles(data: List[float]):
    if not data:
        return {"p50": 0, "p95": 0, "p99": 0, "mean": 0, "std": 0}
    sorted_d = sorted(data)
    n = len(sorted_d)
    
    def get_p(p):
        k = (n - 1) * (p / 100.0)
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            return sorted_d[int(k)]
        d0 = sorted_d[int(f)] * (c - k)
        d1 = sorted_d[int(c)] * (k - f)
        return d0 + d1

    mean = sum(sorted_d) / n
    variance = sum((x - mean) ** 2 for x in sorted_d) / n if n > 1 else 0
    std = math.sqrt(variance)

    return {
        "p50": get_p(50),
        "p95": get_p(95),
        "p99": get_p(99),
        "mean": mean,
        "std": std,
    }

def run_election_trial(target_addr: str, timeout: float = 6.0) -> dict:
    t0 = time.time()
    with grpc.insecure_channel(target_addr) as channel:
        stub = election_pb2_grpc.RingServiceStub(channel)
        res = stub.TriggerElection(
            election_pb2.TriggerRequest(reason="experiment_trial"),
            timeout=timeout
        )
        if not res.started:
            return {"success": False, "error": "Eleição não iniciada (já em andamento?)"}

    while time.time() - t0 < timeout:
        time.sleep(0.1)
        try:
            with grpc.insecure_channel(target_addr) as channel:
                stub = election_pb2_grpc.RingServiceStub(channel)
                st = stub.GetStatus(election_pb2.StatusRequest(), timeout=1.0)
                if not st.election_in_progress and st.current_leader > 0:
                    convergence_time_ms = (time.time() - t0) * 1000.0
                    return {
                        "success": True,
                        "leader": st.current_leader,
                        "leader_addr": st.current_leader_address,
                        "latency_ms": convergence_time_ms,
                        "active_nodes": list(st.known_active_nodes)
                    }
        except Exception:
            pass

    return {"success": False, "error": "Timeout aguardando convergência"}

def run_experiment_suite(target_node="no_1:50051", rounds=10):
    print("=" * 75)
    print(f"📊 INICIANDO BATERIA DE EXPERIMENTOS: {rounds} RODADAS (ANEL DE 10 NÓS)")
    print(f"Nó iniciador alvo: {target_node}")
    print("=" * 75)

    latencies = []
    leaders = []

    for i in range(1, rounds + 1):
        print(f"Executando rodada {i}/{rounds}...", end=" ", flush=True)
        res = run_election_trial(target_node)
        if res["success"]:
            lat = res["latency_ms"]
            latencies.append(lat)
            leaders.append(res["leader"])
            print(f"✅ OK! Líder: Nó {res['leader']} ({res.get('leader_addr')}) | Latência: {lat:.2f} ms")
        else:
            print(f"❌ Falha: {res.get('error')}")
        time.sleep(1.0)

    stats = calculate_percentiles(latencies)
    print("\n" + "=" * 75)
    print("📈 RESULTADOS EXPERIMENTAIS ESTATÍSTICOS (Para o Relatório IEEE)")
    print("=" * 75)
    print(f"Total de rodadas com sucesso: {len(latencies)} / {rounds}")
    if leaders:
        print(f"Líder consistente eleito: Nó {leaders[-1]}")
    print(f"Média:   {stats['mean']:.2f} ms")
    print(f"Desvio:  {stats['std']:.2f} ms")
    print(f"p50:     {stats['p50']:.2f} ms (mediana)")
    print(f"p95:     {stats['p95']:.2f} ms")
    print(f"p99:     {stats['p99']:.2f} ms")
    print("=" * 75)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Automação experimental do Algoritmo do Anel")
    parser.add_argument("--node", default="no_1:50051", help="Endereço do nó alvo")
    parser.add_argument("--rounds", type=int, default=10, help="Número de rodadas para teste")
    args = parser.parse_args()

    run_experiment_suite(target_node=args.node, rounds=args.rounds)
