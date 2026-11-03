#!/usr/bin/env python3
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))
import client_2pc

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="CLI de Transações Bancárias 2PC (MC714)")
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("balances", help="Consulta saldos e locks em todas as réplicas")

    sync_parser = subparsers.add_parser("sync", help="Sincroniza saldos contábeis e desobstrui transações pendentes")
    sync_parser.add_argument("--node", default=None, help="Endereço do nó específico ou None para todos")

    tx_parser = subparsers.add_parser("transfer", help="Efetua uma transferência bancária 2PC")
    tx_parser.add_argument("--source", default="conta_A", help="Conta de origem (default: conta_A)")
    tx_parser.add_argument("--dest", default="conta_B", help="Conta de destino (default: conta_B)")
    tx_parser.add_argument("--amount", type=float, required=True, help="Valor a transferir")
    tx_parser.add_argument("--node", default=None, help="Endereço do nó (opcional, busca o líder automaticamente)")
    tx_parser.add_argument("--crash-leader", action="store_true", help="Simula crash do líder pós-Fase 1")

    res_parser = subparsers.add_parser("resolve", help="Resolve transação bloqueada pelo líder anterior")
    res_parser.add_argument("--tx-id", default=None, help="ID da transação bloqueada (opcional: se omitido, detecta e resolve todas)")
    res_parser.add_argument("--action", default="ABORT", choices=["ABORT", "COMMIT"], help="Ação a tomar (default: ABORT)")

    args = parser.parse_args()

    if args.command == "balances":
        client_2pc.print_bank_balances()
    elif args.command == "sync":
        client_2pc.sync_nodes(args.node)
    elif args.command == "transfer":
        client_2pc.submit_transfer(args.source, args.dest, args.amount, args.node, args.crash_leader)
    elif args.command == "resolve":
        client_2pc.resolve_transaction(args.tx_id, args.action)
    else:
        parser.print_help()

