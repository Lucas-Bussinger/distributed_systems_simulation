#!/usr/bin/env python3
"""
Compila protos/election.proto gerando election_pb2.py e election_pb2_grpc.py
"""
import os
import sys

def compile_proto():
    try:
        from grpc_tools import protoc
    except ImportError:
        print("[ERRO] grpc_tools não encontrado. Certifique-se de que requirements.txt está instalado.")
        return 1

    base_dir = os.path.dirname(os.path.abspath(__file__))
    protos_dir = os.path.join(base_dir, "protos")
    proto_file = os.path.join(protos_dir, "election.proto")

    if not os.path.exists(proto_file):
        print(f"[ERRO] Arquivo proto não encontrado em {proto_file}")
        return 1

    print(f"[PROTOC] Compilando {proto_file}...")
    args = [
        "grpc_tools.protoc",
        f"-I{protos_dir}",
        f"--python_out={base_dir}",
        f"--grpc_python_out={base_dir}",
        proto_file,
    ]

    res = protoc.main(args)
    if res == 0:
        print("[PROTOC] Arquivos election_pb2.py e election_pb2_grpc.py gerados com sucesso!")
    else:
        print(f"[PROTOC] Erro ao compilar proto: código {res}")
    return res

if __name__ == "__main__":
    sys.exit(compile_proto())
