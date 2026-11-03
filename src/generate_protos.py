#!/usr/bin/env python3
"""
Compila protos/election.proto e protos/bank_2pc.proto
gerando election_pb2.py, election_pb2_grpc.py, bank_2pc_pb2.py e bank_2pc_pb2_grpc.py
tanto em src/ quanto na raiz do projeto para compatibilidade total.
"""
import os
import sys

def compile_proto(force: bool = False):
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) if os.path.basename(os.path.dirname(os.path.abspath(__file__))) == "src" else os.path.dirname(os.path.abspath(__file__))
    protos_dir = os.path.join(base_dir, "protos")
    src_dir = os.path.join(base_dir, "src")

    # Se os stubs já existem e não é force, retorna sucesso imediatamente
    expected_files = [
        os.path.join(src_dir, "election_pb2.py"),
        os.path.join(src_dir, "election_pb2_grpc.py"),
        os.path.join(src_dir, "bank_2pc_pb2.py"),
        os.path.join(src_dir, "bank_2pc_pb2_grpc.py"),
    ]
    if not force and all(os.path.exists(f) and os.path.getsize(f) > 0 for f in expected_files):
        return 0

    try:
        from grpc_tools import protoc
    except ImportError:
        print("[ERRO] grpc_tools não encontrado. Instale requirements.txt ou execute dentro do container Docker.")
        return 1

    proto_files = ["election.proto", "bank_2pc.proto"]

    for pf in proto_files:
        proto_path = os.path.join(protos_dir, pf)
        if not os.path.exists(proto_path):
            print(f"[ERRO] Arquivo proto não encontrado em {proto_path}")
            return 1

        print(f"[PROTOC] Compilando {proto_path}...")
        for out_dir in [src_dir, base_dir]:
            os.makedirs(out_dir, exist_ok=True)
            args = [
                "grpc_tools.protoc",
                f"-I{protos_dir}",
                f"--python_out={out_dir}",
                f"--grpc_python_out={out_dir}",
                proto_path,
            ]
            res = protoc.main(args)
            if res != 0:
                print(f"[PROTOC] Erro ao compilar {pf}: código {res}")
                return res

    print("[PROTOC] Stubs election_pb2 e bank_2pc_pb2 gerados com sucesso!")
    return 0

if __name__ == "__main__":
    force_flag = "--force" in sys.argv
    sys.exit(compile_proto(force=force_flag))
