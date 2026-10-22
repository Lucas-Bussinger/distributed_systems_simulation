#!/usr/bin/env python3
"""
Implementação do Algoritmo de Eleição em Anel (Ring Election Algorithm)
Referência: van Steen e Tanenbaum - Distributed Systems (MC714 - Unicamp)
Suporta 10 nós, IDs aleatórios dinâmicos e tolerância a falhas por salto no anel.
"""

import os
import sys
import time
import socket
import logging
import random
import threading
from concurrent import futures
from typing import List, Tuple, Optional
import grpc

# Tenta importar os protos compilados. Se não existirem ou estiverem desatualizados, compila automaticamente.
try:
    import election_pb2
    import election_pb2_grpc
    if not hasattr(election_pb2, "NodeInfo"):
        raise ImportError("Proto desatualizado sem NodeInfo")
except (ImportError, AttributeError):
    print("[INIT] Compilando protos atualizados (com NodeInfo)...")
    from generate_protos import compile_proto
    if compile_proto() != 0:
        print("[ERRO FATAL] Falha na compilação do proto.")
        sys.exit(1)
    import election_pb2
    import election_pb2_grpc

# Configuração de logging estruturado
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [Nó %(name)s] %(message)s",
    datefmt="%H:%M:%S",
)

class RingNode(election_pb2_grpc.RingServiceServicer):
    def __init__(self):
        self.port = int(os.getenv("PORT", "50051"))
        
        # 1. Identificação de Endereço Local
        my_addr_env = os.getenv("MY_ADDR", "")
        if not my_addr_env:
            hostname = socket.gethostname()
            my_addr_env = f"{hostname}:{self.port}"
        self.my_addr = my_addr_env

        # 2. Geração de ID do Nó (Aleatório ou Definido)
        node_id_env = os.getenv("NODE_ID", "random")
        if node_id_env.lower() in ("random", "", "auto"):
            # Extrai índice do host (ex: no_7 -> 7) para garantir unicidade e variedade
            extracted_idx = "".join([c for c in self.my_addr.split(":")[0] if c.isdigit()])
            idx_num = int(extracted_idx) if extracted_idx else 1
            # Gera ID aleatório não-sequencial único entre 100 e 9999
            r = random.SystemRandom()
            self.node_id = r.randint(10, 990) * 10 + (idx_num % 10)
        else:
            self.node_id = int(node_id_env)

        # Configurações de timeouts
        self.rpc_timeout = float(os.getenv("RPC_TIMEOUT", "2.0"))
        self.heartbeat_interval = float(os.getenv("HEARTBEAT_INTERVAL", "3.0"))
        self.heartbeat_timeout = float(os.getenv("HEARTBEAT_TIMEOUT", "1.5"))
        self.missed_heartbeats_limit = int(os.getenv("MISSED_HEARTBEATS_LIMIT", "2"))
        self.startup_grace_period = float(os.getenv("STARTUP_GRACE_PERIOD", "5.0"))
        
        self.logger = logging.getLogger(f"{self.my_addr.split(':')[0]}(ID={self.node_id})")
        
        # 3. Topologia do Anel (Lista de endereços na ordem do anel)
        raw_topology = os.getenv(
            "RING_TOPOLOGY",
            "no_1:50051,no_2:50051,no_3:50051,no_4:50051,no_5:50051,"
            "no_6:50051,no_7:50051,no_8:50051,no_9:50051,no_10:50051"
        )
        self.ring_addresses: List[str] = [p.strip() for p in raw_topology.split(",") if p.strip()]
        
        # 4. Estado interno do nó
        self.lock = threading.Lock()
        self.current_leader: Optional[int] = None
        self.current_leader_address: Optional[str] = None
        self.election_in_progress = False
        self.election_start_time: float = 0.0
        self.known_active_nodes: List[int] = []
        self.missed_heartbeats = 0
        self.running = True

        self.logger.info(
            f"🎲 Nó inicializado com ID ALEATÓRIO = {self.node_id} | Endereço = {self.my_addr}\n"
            f"   Topologia em anel configurada ({len(self.ring_addresses)} nós): {self.ring_addresses}"
        )

    def get_successors_in_order(self) -> List[str]:
        """
        Retorna a lista de endereços candidatos a sucessor na ordem circular do anel.
        Exemplo: para no_4 em um anel de 10 nós, retorna:
        [no_5, no_6, no_7, no_8, no_9, no_10, no_1, no_2, no_3]
        """
        match_idx = None
        for i, addr in enumerate(self.ring_addresses):
            if addr == self.my_addr or addr.split(":")[0] == self.my_addr.split(":")[0]:
                match_idx = i
                break

        if match_idx is None:
            return [a for a in self.ring_addresses if a != self.my_addr]

        ordered = self.ring_addresses[match_idx + 1:] + self.ring_addresses[:match_idx]
        return [a for a in ordered if a != self.my_addr]

    # =========================================================================
    # Implementação dos Endpoints gRPC (Serviço RingService)
    # =========================================================================

    def PassElection(self, request, context):
        """
        Recebe a mensagem de eleição circulando pelo anel.
        """
        initiator = request.initiator_id
        active_nodes = list(request.active_nodes)
        active_infos = list(getattr(request, "active_node_infos", []))
        hops = request.hop_count
        start_t = request.start_time

        self.logger.info(
            f"📨 [RECEBEU ELEIÇÃO] Iniciador: Nó {initiator} | "
            f"Nós ativos até agora: {active_nodes} | Salto: {hops}"
        )

        threading.Thread(
            target=self._process_election_step,
            args=(initiator, active_nodes, active_infos, hops, start_t),
            daemon=True
        ).start()

        return election_pb2.ElectionResponse(accepted=True, message="OK")

    def AnnounceCoordinator(self, request, context):
        """
        Recebe o anúncio do novo coordenador circulando pelo anel.
        """
        leader_id = request.leader_id
        leader_addr = getattr(request, "leader_address", "")
        announcer_id = request.announcer_id
        hops = request.hop_count
        active_nodes = list(request.active_nodes)
        active_infos = list(getattr(request, "active_node_infos", []))
        start_t = request.start_time

        self.logger.info(
            f"📢 [RECEBEU COORDENADOR] Líder Eleito: Nó {leader_id} ({leader_addr}) | "
            f"Anunciador: Nó {announcer_id} | Salto: {hops}"
        )

        threading.Thread(
            target=self._process_coordinator_step,
            args=(leader_id, leader_addr, announcer_id, hops, active_nodes, active_infos, start_t),
            daemon=True
        ).start()

        return election_pb2.CoordinatorResponse(accepted=True, message="OK")

    def Ping(self, request, context):
        """
        Endpoint de Heartbeat respondido pelo líder.
        """
        return election_pb2.PingResponse(
            node_id=self.node_id,
            current_leader=self.current_leader or 0,
            status="ALIVE"
        )

    def TriggerElection(self, request, context):
        """
        Permite disparar a eleição manualmente.
        """
        self.logger.info(f"⚡ [TRIGGER] Eleição disparada manualmente via RPC. Motivo: {request.reason}")
        started = self.start_election(reason=f"manual_trigger ({request.reason})")
        return election_pb2.TriggerResponse(
            started=started,
            message="Eleição iniciada" if started else "Eleição já estava em andamento"
        )

    def GetStatus(self, request, context):
        """
        Retorna o status atual do nó para auditoria e métricas.
        """
        with self.lock:
            return election_pb2.StatusResponse(
                node_id=self.node_id,
                address=self.my_addr,
                current_leader=self.current_leader or 0,
                current_leader_address=self.current_leader_address or "",
                is_leader=(self.current_leader == self.node_id),
                election_in_progress=self.election_in_progress,
                known_active_nodes=self.known_active_nodes,
                state="LEADER" if self.current_leader == self.node_id else "FOLLOWER"
            )

    # =========================================================================
    # Lógica Central do Algoritmo do Anel (van Steen & Tanenbaum)
    # =========================================================================

    def start_election(self, reason: str = "") -> bool:
        """
        Inicia uma nova rodada de eleição a partir deste nó.
        """
        with self.lock:
            if self.election_in_progress:
                self.logger.warning(f"Eleição já está em andamento. Ignorando novo início ({reason}).")
                return False
            self.election_in_progress = True
            self.election_start_time = time.time()

        self.logger.info(f"🚀 [INICIANDO ELEIÇÃO] Motivo: {reason}. Enviando mensagem inicial com Nó {self.node_id} ({self.my_addr})...")
        
        initial_info = [election_pb2.NodeInfo(node_id=self.node_id, address=self.my_addr)]
        threading.Thread(
            target=self._forward_election_message,
            args=(self.node_id, [self.node_id], initial_info, 1, self.election_start_time),
            daemon=True
        ).start()
        return True

    def _process_election_step(self, initiator: int, active_nodes: List[int], active_infos: list, hops: int, start_t: float):
        """
        Processa o passo da eleição ao receber a mensagem.
        """
        # CASO 1: A mensagem deu a volta completa e retornou ao iniciador!
        if initiator == self.node_id:
            elapsed_ms = (time.time() - start_t) * 1000.0
            
            # Escolhe o nó com o maior ID aleatório
            if active_infos:
                best_info = max(active_infos, key=lambda x: x.node_id)
                elected_leader = best_info.node_id
                elected_addr = best_info.address
            else:
                elected_leader = max(active_nodes)
                elected_addr = ""

            self.logger.info(
                f"\n🏆 ==================== ELEIÇÃO CONCLUÍDA ====================\n"
                f"   Volta completa no anel confirmada pelo iniciador Nó {self.node_id}!\n"
                f"   Nós ativos participantes: {active_nodes}\n"
                f"   LÍDER ELEITO (Maior ID Aleatório): Nó {elected_leader} em {elected_addr}\n"
                f"   Métricas: Saltos = {hops} | Tempo de Convergência = {elapsed_ms:.2f} ms\n"
                f"============================================================"
            )

            with self.lock:
                self.current_leader = elected_leader
                self.current_leader_address = elected_addr
                self.election_in_progress = False
                self.known_active_nodes = sorted(active_nodes)
                self.missed_heartbeats = 0

            # Dispara anúncio do coordenador para todos no anel
            self.logger.info(f"📣 [ANÚNCIO] Propagando COORDENADOR (Líder: Nó {elected_leader} em {elected_addr}) ao redor do anel...")
            self._forward_coordinator_message(
                leader_id=elected_leader,
                leader_addr=elected_addr,
                announcer_id=self.node_id,
                hops=1,
                active_nodes=active_nodes,
                active_infos=active_infos,
                start_t=time.time()
            )
            return

        # CASO 2: Nó intermediário
        with self.lock:
            self.election_in_progress = True
            self.current_leader = None
            if self.node_id not in active_nodes:
                active_nodes.append(self.node_id)
                active_infos.append(election_pb2.NodeInfo(node_id=self.node_id, address=self.my_addr))

        self.logger.info(
            f"➡️ [ENCAMINHANDO ELEIÇÃO] Adicionado Nó {self.node_id} ({self.my_addr}). "
            f"Nova lista: {active_nodes}. Passando ao próximo nó vivo no anel..."
        )

        self._forward_election_message(initiator, active_nodes, active_infos, hops + 1, start_t)

    def _forward_election_message(self, initiator: int, active_nodes: List[int], active_infos: list, hops: int, start_t: float):
        """
        Encaminha mensagem de eleição tentando os sucessores na ordem circular do anel.
        Pula sucessores caídos automaticamente (crash-stop).
        """
        successors = self.get_successors_in_order()
        message = election_pb2.ElectionMessage(
            initiator_id=initiator,
            active_nodes=active_nodes,
            hop_count=hops,
            start_time=start_t,
            active_node_infos=active_infos
        )

        for next_addr in successors:
            try:
                self.logger.info(f"Tentando repassar eleição para sucessor {next_addr}...")
                with grpc.insecure_channel(next_addr) as channel:
                    stub = election_pb2_grpc.RingServiceStub(channel)
                    resp = stub.PassElection(message, timeout=self.rpc_timeout)
                    if resp.accepted:
                        self.logger.info(f"✅ Eleição entregue com sucesso a {next_addr}.")
                        return True
            except grpc.RpcError as e:
                self.logger.warning(
                    f"⚠️ [FALHA NO ANEL] Sucessor {next_addr} inacessível: {e.code()}. "
                    f"PULANDO {next_addr} e tentando o próximo no anel..."
                )

        self.logger.error("❌ Nenhum sucessor no anel respondeu! Elegendo-se a si próprio.")
        with self.lock:
            self.current_leader = self.node_id
            self.current_leader_address = self.my_addr
            self.election_in_progress = False
            self.known_active_nodes = [self.node_id]
        return False

    def _process_coordinator_step(self, leader_id: int, leader_addr: str, announcer_id: int, hops: int, active_nodes: List[int], active_infos: list, start_t: float):
        """
        Processa o anúncio do coordenador.
        """
        if announcer_id == self.node_id:
            elapsed_ms = (time.time() - start_t) * 1000.0
            self.logger.info(
                f"✅ [ANÚNCIO FINALIZADO] A confirmação do líder Nó {leader_id} ({leader_addr}) "
                f"percorreu todo o anel! (Saltos: {hops}, Tempo: {elapsed_ms:.2f} ms)"
            )
            return

        with self.lock:
            self.current_leader = leader_id
            self.current_leader_address = leader_addr
            self.election_in_progress = False
            self.known_active_nodes = sorted(active_nodes)
            self.missed_heartbeats = 0

        self.logger.info(
            f"👑 [NOVO LÍDER ACEITO] Nó {self.node_id} reconhece formalmente Nó {leader_id} ({leader_addr}) como LÍDER! "
            f"Repassando anúncio..."
        )

        self._forward_coordinator_message(leader_id, leader_addr, announcer_id, hops + 1, active_nodes, active_infos, start_t)

    def _forward_coordinator_message(self, leader_id: int, leader_addr: str, announcer_id: int, hops: int, active_nodes: List[int], active_infos: list, start_t: float):
        """
        Encaminha mensagem de coordenador para o próximo nó vivo no anel.
        """
        successors = self.get_successors_in_order()
        message = election_pb2.CoordinatorMessage(
            leader_id=leader_id,
            leader_address=leader_addr,
            announcer_id=announcer_id,
            hop_count=hops,
            active_nodes=active_nodes,
            start_time=start_t,
            active_node_infos=active_infos
        )

        for next_addr in successors:
            try:
                with grpc.insecure_channel(next_addr) as channel:
                    stub = election_pb2_grpc.RingServiceStub(channel)
                    resp = stub.AnnounceCoordinator(message, timeout=self.rpc_timeout)
                    if resp.accepted:
                        return True
            except grpc.RpcError as e:
                self.logger.warning(
                    f"⚠️ [FALHA NO ANEL] Falha ao entregar anúncio a {next_addr}: {e.code()}. Pulando..."
                )
        return False

    # =========================================================================
    # Monitor de Falhas (Heartbeat periódico para o Líder)
    # =========================================================================

    def _heartbeat_loop(self):
        """
        Thread de monitoramento de saúde do líder.
        """
        self.logger.info(f"Monitor de Heartbeat ativo (intervalo={self.heartbeat_interval}s). Aguardando {self.startup_grace_period}s...")
        time.sleep(self.startup_grace_period)

        # Jitter para balancear a primeira eleição
        extracted_idx = "".join([c for c in self.my_addr.split(":")[0] if c.isdigit()])
        idx_num = int(extracted_idx) if extracted_idx else 1
        time.sleep(idx_num * 0.4)

        while self.running:
            time.sleep(self.heartbeat_interval)

            with self.lock:
                leader = self.current_leader
                leader_addr = self.current_leader_address
                in_progress = self.election_in_progress

            # Cenário 1: Sem líder definido -> Dispara eleição inicial
            if leader is None or not leader_addr:
                if not in_progress:
                    self.logger.info("Nenhum líder configurado após inicialização. Disparando eleição...")
                    self.start_election(reason="boot_without_leader")
                continue

            # Cenário 2: Sou o líder -> Não envio ping para mim mesmo
            if leader == self.node_id or leader_addr == self.my_addr:
                continue

            # Cenário 3: Sou seguidor -> Envia ping para o endereço do líder
            try:
                with grpc.insecure_channel(leader_addr) as channel:
                    stub = election_pb2_grpc.RingServiceStub(channel)
                    stub.Ping(election_pb2.PingRequest(sender_id=self.node_id), timeout=self.heartbeat_timeout)
                    with self.lock:
                        if self.missed_heartbeats > 0:
                            self.logger.info(f"Heartbeat para Líder Nó {leader} ({leader_addr}) restabelecido.")
                        self.missed_heartbeats = 0

            except grpc.RpcError as e:
                with self.lock:
                    self.missed_heartbeats += 1
                    failures = self.missed_heartbeats

                self.logger.warning(
                    f"⚠️ [FALHA DE HEARTBEAT] Líder Nó {leader} ({leader_addr}) não respondeu: {e.code()} "
                    f"({failures}/{self.missed_heartbeats_limit})"
                )

                if failures >= self.missed_heartbeats_limit:
                    self.logger.error(
                        f"🚨 [LÍDER MORTO DETECTADO] Líder Nó {leader} ({leader_addr}) falhou {failures} heartbeats consecutivos! "
                        f"Iniciando nova eleição com salto sobre o líder caído..."
                    )
                    with self.lock:
                        self.current_leader = None
                        self.current_leader_address = None
                        self.missed_heartbeats = 0
                    self.start_election(reason=f"leader_{leader}_crashed")

    # =========================================================================
    # Inicialização do Servidor
    # =========================================================================

    def serve(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        election_pb2_grpc.add_RingServiceServicer_to_server(self, server)
        bind_addr = f"0.0.0.0:{self.port}"
        server.add_insecure_port(bind_addr)
        server.start()
        self.logger.info(f"🚀 Servidor gRPC escutando em {bind_addr}...")

        hb_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        hb_thread.start()

        try:
            server.wait_for_termination()
        except KeyboardInterrupt:
            self.logger.info("Encerrando nó...")
            self.running = False
            server.stop(0)

if __name__ == "__main__":
    node = RingNode()
    node.serve()
