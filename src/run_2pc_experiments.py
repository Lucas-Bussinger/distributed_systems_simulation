#!/usr/bin/env python3
"""
===============================================================================
MC714 - Sistemas Distribuídos (Unicamp) - Trabalho 2
Script de Coleta de Métricas Experimentais de Latência do 2PC
===============================================================================
Mede o tempo total de execução da transação (Fase 1 + Fase 2) no cenário normal.
Calcula Média, Desvio Padrão, Mediana (p50), p95 e p99 para o Relatório IEEE.
===============================================================================
"""

import sys
import os
import time
import math
import argparse
from typing import List, Dict
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

def calculate_percentiles(data: List[float]) -> Dict[str, float]:
    if not data:
        return {"p50": 0, "p95": 0, "p99": 0, "mean": 0, "std": 0, "min": 0, "max": 0}
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
        "mean": mean,
        "std": std,
        "p50": get_p(50),
        "p95": get_p(95),
        "p99": get_p(99),
        "min": sorted_d[0],
        "max": sorted_d[-1],
    }

def discover_cluster():
    active = []
    leader_addr = None
    leader_id = None
    for cname, addr in DEFAULT_NODES:
        try:
            with grpc.insecure_channel(addr) as ch:
                stub = election_pb2_grpc.RingServiceStub(ch)
                st = stub.GetStatus(election_pb2.StatusRequest(), timeout=1.0)
                active.append((cname, addr, st.node_id))
                if st.is_leader:
                    leader_addr = addr
                    leader_id = st.node_id
                elif not leader_addr and st.current_leader_address:
                    leader_addr = st.current_leader_address
                    leader_id = st.current_leader
        except Exception:
            pass
    return active, leader_id, leader_addr

def run_experiment_suite(rounds: int = 20, delay_between_rounds: float = 0.5):
    print("=" * 80)
    print("📊 BATERIA EXPERIMENTAL: LATÊNCIA DO PROTOCOLO 2PC BANCÁRIO (MC714)")
    print("=" * 80)

    active_nodes, leader_id, leader_addr = discover_cluster()
    num_nodes = len(active_nodes)
    print(f"Cluster detectado: {num_nodes} nós ativos {[(n[0], n[2]) for n in active_nodes]}")
    print(f"Líder Coordenador: Nó {leader_id} em {leader_addr}")
    print(f"Total de rodadas programadas: {rounds}")
    print("=" * 80)

    if not leader_addr:
        print("❌ Erro: Nenhum líder ativo encontrado!")
        sys.exit(1)

    latencies = []
    successes = 0

    with grpc.insecure_channel(leader_addr) as channel:
        stub = bank_2pc_pb2_grpc.TwoPhaseCommitServiceStub(channel)

        for i in range(1, rounds + 1):
            print(f"Rodada {i:2d}/{rounds}: Executando 2PC...", end=" ", flush=True)
            # Alterna valores e contas para movimentar saldos
            amount = 1.0 + (i % 10)
            req = bank_2pc_pb2.ClientTransactionRequest(
                source_account="conta_A" if i % 2 == 0 else "conta_B",
                destination_account="conta_B" if i % 2 == 0 else "conta_A",
                amount=amount,
                test_crash_leader_after_prepare=False
            )
            try:
                res = stub.SubmitTransaction(req, timeout=10.0)
                if res.success and res.status == "COMMITTED":
                    lat = res.latency_ms
                    latencies.append(lat)
                    successes += 1
                    print(f"✅ COMMIT ({res.transaction_id}) | Latência: {lat:6.2f} ms")
                else:
                    print(f"❌ Falha: {res.message}")
            except Exception as e:
                print(f"❌ Erro RPC: {e}")

            time.sleep(delay_between_rounds)

    stats = calculate_percentiles(latencies)

    print("\n" + "=" * 80)
    print(f"📈 RESULTADOS ESTATÍSTICOS DA LATÊNCIA DO 2PC ({num_nodes} NÓS)")
    print("=" * 80)
    print(f"Nós ativos no cluster:     {num_nodes}")
    print(f"Rodadas com sucesso:       {successes} / {rounds} ({successes/rounds*100:.1f}%)")
    print(f"Média:                     {stats['mean']:6.2f} ms")
    print(f"Desvio Padrão:             {stats['std']:6.2f} ms")
    print(f"Mínimo:                    {stats['min']:6.2f} ms")
    print(f"Mediana (p50):             {stats['p50']:6.2f} ms")
    print(f"Percentil 95 (p95):        {stats['p95']:6.2f} ms")
    print(f"Percentil 99 (p99):        {stats['p99']:6.2f} ms")
    print(f"Máximo:                    {stats['max']:6.2f} ms")
    print("=" * 80)

    print("\n📝 FORMATO PARA TABELA DO RELATÓRIO IEEE:")
    print("--------------------------------------------------------------------------------")
    print(f"Topologia: {num_nodes} Nós | Média: {stats['mean']:.2f}ms | p50: {stats['p50']:.2f}ms | p95: {stats['p95']:.2f}ms | Desvio: {stats['std']:.2f}ms")
    print("--------------------------------------------------------------------------------\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Coleta experimental de latência do 2PC")
    parser.add_argument("--rounds", type=int, default=20, help="Número de rodadas (default: 20)")
    parser.add_argument("--delay", type=float, default=0.3, help="Intervalo entre transações em segundos")
    args = parser.parse_args()

    run_experiment_suite(rounds=args.rounds, delay_between_rounds=args.delay)

