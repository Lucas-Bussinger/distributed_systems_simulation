#!/usr/bin/env python3
"""
CLI de gerenciamento bancário e submissão de transações 2PC.
Permite consultar saldos/locks em todos os nós e disparar transferências.
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

def get_current_leader(nodes=DEFAULT_NODES):
    """Encontra o nó que atualmente é o Líder"""
    for cname, addr in nodes:
        try:
            with grpc.insecure_channel(addr) as channel:
                stub = election_pb2_grpc.RingServiceStub(channel)
                res = stub.GetStatus(election_pb2.StatusRequest(), timeout=1.5)
                if res.is_leader:
                    return res.node_id, addr
                if res.current_leader_address:
                    return res.current_leader, res.current_leader_address
        except Exception:
            pass
    return None, None

def print_bank_balances(nodes=DEFAULT_NODES):
    """Exibe uma tabela comparativa dos saldos e locks em todas as réplicas"""
    print("=" * 90)
    print(f"{'CONTAINER':<10} | {'NÓ ID':<10} | {'ENDEREÇO':<14} | {'CONTA A':<11} | {'CONTA B':<11} | {'CONTA C':<11} | {'LOCKS/PENDENTES'}")
    print("=" * 90)

    for cname, addr in nodes:
        try:
            with grpc.insecure_channel(addr) as channel:
                stub = bank_2pc_pb2_grpc.TwoPhaseCommitServiceStub(channel)
                res = stub.GetBankState(bank_2pc_pb2.BankStateRequest(), timeout=2.0)
                accs = {acc.account_id: (acc.balance, acc.is_locked, acc.locked_by_tx) for acc in res.accounts}
                
                a_str = f"R$ {accs.get('conta_A', (0,0))[0]:.2f}" + (" 🔒" if accs.get('conta_A', (0,0, ''))[1] else "")
                b_str = f"R$ {accs.get('conta_B', (0,0))[0]:.2f}" + (" 🔒" if accs.get('conta_B', (0,0, ''))[1] else "")
                c_str = f"R$ {accs.get('conta_C', (0,0))[0]:.2f}" + (" 🔒" if accs.get('conta_C', (0,0, ''))[1] else "")

                pending_str = f"{res.pending_transactions_count} pendente(s)"
                if res.pending_tx_ids:
                    pending_str += f" ({', '.join(res.pending_tx_ids)})"

                print(f"{cname:<10} | {res.node_id:<10} | {addr:<14} | {a_str:<11} | {b_str:<11} | {c_str:<11} | {pending_str}")
        except grpc.RpcError as e:
            print(f"{cname:<10} | {'-':<10} | {addr:<14} | {'🔴 OFFLINE':<11} | {'-':<11} | {'-':<11} | {e.code()}")
        except Exception as ex:
            print(f"{cname:<10} | {'-':<10} | {addr:<14} | {'🔴 ERRO':<11} | {'-':<11} | {'-':<11} | {ex}")
    print("=" * 90)

def submit_transfer(source, dest, amount, target_node=None, crash_leader=False):
    """Envia uma transação bancária 2PC ao líder"""
    if not target_node:
        leader_id, leader_addr = get_current_leader()
        if not leader_addr:
            print("❌ Erro: Nenhum líder ativo encontrado no cluster!")
            return False
        target_node = leader_addr
        print(f"🎯 Líder identificado: Nó {leader_id} em {leader_addr}")

    print(f"💸 Submetendo transação: Debitar R$ {amount:.2f} de '{source}' -> Creditar em '{dest}' no nó {target_node}...")
    try:
        with grpc.insecure_channel(target_node) as channel:
            stub = bank_2pc_pb2_grpc.TwoPhaseCommitServiceStub(channel)
            req = bank_2pc_pb2.ClientTransactionRequest(
                source_account=source,
                destination_account=dest,
                amount=amount,
                test_crash_leader_after_prepare=crash_leader
            )
            res = stub.SubmitTransaction(req, timeout=10.0)
            print("-" * 60)
            print(f"ID da Transação: {res.transaction_id}")
            print(f"Status Global:   {res.status}")
            print(f"Sucesso:         {'✅ SIM' if res.success else '❌ NÃO'}")
            print(f"Mensagem:        {res.message}")
            print(f"Latência 2PC:    {res.latency_ms:.2f} ms")
            print("-" * 60)
            return res.success
    except grpc.RpcError as e:
        print(f"❌ Erro RPC ao submeter transação: {e.code()} - {e.details()}")
        return False

def resolve_transaction(tx_id, action="ABORT", target_node=None):
    """Resolve uma transação bloqueada usando o líder atual"""
    if not target_node:
        leader_id, leader_addr = get_current_leader()
        if not leader_addr:
            print("❌ Erro: Nenhum líder ativo encontrado!")
            return False
        target_node = leader_addr
        print(f"🎯 Novo Líder para resolução: Nó {leader_id} em {leader_addr}")

    try:
        with grpc.insecure_channel(target_node) as channel:
            stub = bank_2pc_pb2_grpc.TwoPhaseCommitServiceStub(channel)
            res = stub.ResolveBlockedTransaction(bank_2pc_pb2.ResolveRequest(
                transaction_id=tx_id,
                action=action
            ), timeout=5.0)
            print(f"✅ Transação {tx_id} resolvida: {res.message}")
            return res.resolved
    except Exception as e:
        print(f"❌ Erro ao resolver transação: {e}")
        return False

def sync_nodes(target_node=None):
    """Força sincronização contábil (Crash-Recovery) de um nó ou de todos"""
    targets = [(target_node.split(":")[0], target_node)] if target_node else DEFAULT_NODES
    print("🔄 Forçando sincronização contábil com o cluster (Crash-Recovery)...")
    for cname, addr in targets:
        try:
            with grpc.insecure_channel(addr) as channel:
                stub = bank_2pc_pb2_grpc.TwoPhaseCommitServiceStub(channel)
                res = stub.SyncBankState(bank_2pc_pb2.BankStateRequest(), timeout=2.5)
                accs = {acc.account_id: f"R$ {acc.balance:.2f}" for acc in res.accounts}
                print(f"✅ {cname} ({addr}) sincronizado com sucesso: {accs}")
        except Exception as e:
            print(f"⚠️ {cname} ({addr}): {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CLI de Transações Bancárias 2PC (MC714)")
    subparsers = parser.add_subparsers(dest="command")

    # Comando balances
    subparsers.add_parser("balances", help="Consulta saldos e locks em todas as réplicas")

    # Comando sync
    sync_parser = subparsers.add_parser("sync", help="Sincroniza saldos contábeis pós-recuperação (Crash-Recovery)")
    sync_parser.add_argument("--node", default=None, help="Endereço do nó específico (ex: no_6:50051) ou None para todos")

    # Comando transfer
    tx_parser = subparsers.add_parser("transfer", help="Efetua uma transferência bancária 2PC")
    tx_parser.add_argument("--source", default="conta_A", help="Conta de origem (default: conta_A)")
    tx_parser.add_argument("--dest", default="conta_B", help="Conta de destino (default: conta_B)")
    tx_parser.add_argument("--amount", type=float, required=True, help="Valor a transferir")
    tx_parser.add_argument("--node", default=None, help="Endereço do nó (opcional, busca o líder automaticamente)")
    tx_parser.add_argument("--crash-leader", action="store_true", help="Simula crash do líder pós-Fase 1")

    # Comando resolve
    res_parser = subparsers.add_parser("resolve", help="Resolve transação bloqueada pelo líder anterior")
    res_parser.add_argument("--tx-id", required=True, help="ID da transação bloqueada")
    res_parser.add_argument("--action", default="ABORT", choices=["ABORT", "COMMIT"], help="Ação a tomar")

    args = parser.parse_args()

    if args.command == "balances":
        print_bank_balances()
    elif args.command == "sync":
        sync_nodes(args.node)
    elif args.command == "transfer":
        submit_transfer(args.source, args.dest, args.amount, args.node, args.crash_leader)
    elif args.command == "resolve":
        resolve_transaction(args.tx_id, args.action)
    else:
        parser.print_help()

