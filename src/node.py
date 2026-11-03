#!/usr/bin/env python3
"""
===============================================================================
MC714 - Sistemas Distribuídos (Unicamp) - Trabalho 2
Implementação Integrada:
  1. Algoritmo de Eleição de Líder em Anel (Ring Election - van Steen & Tanenbaum)
  2. Protocolo 2PC (Two-Phase Commit) para Transações Bancárias Distribuídas
===============================================================================

MODELO DO SISTEMA E JUSTIFICATIVAS TEÓRICAS:
-------------------------------------------
1. MODELO TEMPORAL:
   - Parcialmente Síncrono (Partially Synchronous). O sistema opera com tempos
     de transmissão e de processamento não-determinísticos na rede local, mas
     estabelece limites superiores (timeouts/deadlines gRPC explícitos) para
     distinguir nós lentos de nós inativos ou incomunicáveis. Não é necessária
     sincronização de relógios físicos entre os nós.

2. PROPRIEDADES DOS CANAIS E TIMEOUTS gRPC:
   - Comunicação estritamente baseada em gRPC sobre TCP (transporte confiável,
     ordenado FIFO e detecção de perda de pacotes).
   - Como o gRPC por padrão não estabelece prazos para chamadas RPC, todos os
     stubs gRPC (Fase 1 Prepare, Fase 2 Commit/Abort, Heartbeats e Eleição)
     são configurados com deadlines estritos (`timeout=...`).
   - Na Fase 1 do 2PC, qualquer timeout ou erro gRPC (ex: DEADLINE_EXCEEDED,
     UNAVAILABLE) é considerado estritamente como voto "NÃO" pelo Coordenador,
     garantindo a propriedade de Segurança (Safety) do 2PC.

3. MODELO DE FALHAS E PERSISTÊNCIA:
   - Crash-Stop: Nós podem falhar parando sua execução.
   - Tolerância a falhas no anel: Pulo dinâmico (skip) sobre nós inativos durante
     a propagação das mensagens de eleição e anúncio.
   - Detecção de falhas do Líder: Heartbeat periódico com limite de falhas
     consecutivas (missed_heartbeats_limit).

4. TRATAMENTO DO BLOQUEIO INERENTE AO 2PC (BLOCKING PROBLEM):
   - Se o Coordenador falhar após coletar os votos SIM na Fase 1 e antes de
     disparar o Commit da Fase 2, os participantes permanecem no estado PREPARED
     com locks de saldo ativos (comportamento teórico fundamental do 2PC).
   - O detector de falhas identifica a morte do líder e dispara o Algoritmo do
     Anel para eleger um novo Líder. O novo Líder atua como novo Coordenador
     e pode executar o procedimento de recuperação (ResolveBlockedTransaction),
     desbloqueando os saldos com segurança (Abort global de transação não commitada).
===============================================================================
"""

import os
import sys
import time
import json
import socket
import logging
import random
import uuid
import threading
from concurrent import futures
from typing import List, Dict, Optional, Tuple

import grpc

# Importa ou compila protos gRPC com retentativa segura para concorrência
imported = False
for _ in range(5):
    try:
        import election_pb2
        import election_pb2_grpc
        import bank_2pc_pb2
        import bank_2pc_pb2_grpc
        if (hasattr(election_pb2, "NodeInfo") and 
            hasattr(bank_2pc_pb2, "TwoPhaseCommitServiceServicer") and
            hasattr(bank_2pc_pb2_grpc.TwoPhaseCommitServiceServicer, "SyncBankState")):
            imported = True
            break
    except (ImportError, AttributeError):
        pass

    try:
        from generate_protos import compile_proto
    except ImportError:
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from generate_protos import compile_proto
    try:
        compile_proto(force=True)
    except Exception:
        pass
    time.sleep(0.5)

if not imported:
    import election_pb2
    import election_pb2_grpc
    import bank_2pc_pb2
    import bank_2pc_pb2_grpc

# Configuração de logging estruturado
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)

class DistributedBankNode(election_pb2_grpc.RingServiceServicer, bank_2pc_pb2_grpc.TwoPhaseCommitServiceServicer):
    """
    Nó lógico que implementa simultaneamente:
      - RingService (Eleição de líder em anel e heartbeats)
      - TwoPhaseCommitService (Coordenador e Coorte para transações bancárias 2PC)
    """

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
            extracted_idx = "".join([c for c in self.my_addr.split(":")[0] if c.isdigit()])
            idx_num = int(extracted_idx) if extracted_idx else 1
            r = random.SystemRandom()
            self.node_id = r.randint(10, 990) * 10 + (idx_num % 10)
        else:
            self.node_id = int(node_id_env)

        # 3. Configurações de Timeouts e Tolerância
        self.rpc_timeout = float(os.getenv("RPC_TIMEOUT", "2.0"))
        self.two_pc_timeout = float(os.getenv("TWO_PC_TIMEOUT", "2.5"))
        self.heartbeat_interval = float(os.getenv("HEARTBEAT_INTERVAL", "3.0"))
        self.heartbeat_timeout = float(os.getenv("HEARTBEAT_TIMEOUT", "1.5"))
        self.missed_heartbeats_limit = int(os.getenv("MISSED_HEARTBEATS_LIMIT", "2"))
        self.startup_grace_period = float(os.getenv("STARTUP_GRACE_PERIOD", "5.0"))

        container_name = self.my_addr.split(":")[0]
        self.logger = logging.getLogger(f"{container_name}(ID={self.node_id})")

        # 4. Topologia do Anel
        raw_topology = os.getenv(
            "RING_TOPOLOGY",
            "no_1:50051,no_2:50051,no_3:50051,no_4:50051,no_5:50051,"
            "no_6:50051,no_7:50051,no_8:50051,no_9:50051,no_10:50051"
        )
        self.ring_addresses: List[str] = [p.strip() for p in raw_topology.split(",") if p.strip()]

        # 5. Estado Interno de Eleição (Algoritmo do Anel)
        self.ring_lock = threading.Lock()
        self.current_leader: Optional[int] = None
        self.current_leader_address: Optional[str] = None
        self.election_in_progress = False
        self.election_start_time: float = 0.0
        self.known_active_nodes: List[int] = []
        self.known_active_addresses: List[str] = []
        self.missed_heartbeats = 0
        self.running = True

        # 6. Estado Interno do Banco de Dados Réplica e Protocolo 2PC
        self.bank_lock = threading.Lock()
        # Inicialização dos saldos bancários padrão
        self.accounts: Dict[str, float] = {
            "conta_A": 1000.0,
            "conta_B": 1000.0,
            "conta_C": 1000.0,
            "conta_D": 500.0,
        }
        # Tabela de Locks em Memória: mapeia "nome_da_conta" -> "transaction_id" que adquiriu o lock
        self.account_locks: Dict[str, str] = {}
        # Transações em estado PREPARED (pendentes de Fase 2)
        self.pending_transactions: Dict[str, dict] = {}
        # Histórico de transações finalizadas: "transaction_id" -> "COMMITTED" / "ABORTED"
        self.transaction_history: Dict[str, str] = {}

        # Flag indicando se precisa sincronizar com o cluster pós-boot (Crash-Recovery)
        self.sync_needed: bool = True
        # Tenta carregar estado contábil persistido previamente em disco (Crash-Recovery)
        if self._load_local_state():
            self.sync_needed = False

        # Gancho para Injeção de Falhas (Chaos Engineering / Cenários 3 e 4 de teste)
        self.chaos_crash_after_prepare: bool = False
        self.chaos_hang_prepare: bool = False

        self.logger.info(
            f"🎲 Nó iniciado com ID ALEATÓRIO = {self.node_id} | Endereço = {self.my_addr}\n"
            f"   Topologia configurada ({len(self.ring_addresses)} nós): {self.ring_addresses}\n"
            f"   Contas bancárias iniciais: {self.accounts}"
        )

    # =========================================================================
    # SEÇÃO 1: ALGORITMO DE ELEIÇÃO EM ANEL (van Steen & Tanenbaum)
    # =========================================================================

    def get_successors_in_order(self) -> List[str]:
        match_idx = None
        for i, addr in enumerate(self.ring_addresses):
            if addr == self.my_addr or addr.split(":")[0] == self.my_addr.split(":")[0]:
                match_idx = i
                break

        if match_idx is None:
            return [a for a in self.ring_addresses if a != self.my_addr]

        ordered = self.ring_addresses[match_idx + 1:] + self.ring_addresses[:match_idx]
        return [a for a in ordered if a != self.my_addr]

    def PassElection(self, request, context):
        initiator = request.initiator_id
        active_nodes = list(request.active_nodes)
        active_infos = list(getattr(request, "active_node_infos", []))
        hops = request.hop_count
        start_t = request.start_time

        self.logger.info(
            f"📨 [RECEBEU ELEIÇÃO] Iniciador: Nó {initiator} | "
            f"Nós ativos: {active_nodes} | Salto: {hops}"
        )

        threading.Thread(
            target=self._process_election_step,
            args=(initiator, active_nodes, active_infos, hops, start_t),
            daemon=True
        ).start()

        return election_pb2.ElectionResponse(accepted=True, message="OK")

    def AnnounceCoordinator(self, request, context):
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
        return election_pb2.PingResponse(
            node_id=self.node_id,
            current_leader=self.current_leader or 0,
            status="ALIVE"
        )

    def TriggerElection(self, request, context):
        self.logger.info(f"⚡ [TRIGGER] Eleição disparada manualmente via RPC. Motivo: {request.reason}")
        started = self.start_election(reason=f"manual_trigger ({request.reason})")
        return election_pb2.TriggerResponse(
            started=started,
            message="Eleição iniciada" if started else "Eleição já estava em andamento"
        )

    def GetStatus(self, request, context):
        with self.ring_lock:
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

    def start_election(self, reason: str = "") -> bool:
        with self.ring_lock:
            if self.election_in_progress:
                self.logger.warning(f"Eleição já em andamento. Ignorando início ({reason}).")
                return False
            self.election_in_progress = True
            self.election_start_time = time.time()

        self.logger.info(f"🚀 [INICIANDO ELEIÇÃO] Motivo: {reason}. Enviando Nó {self.node_id} ({self.my_addr})...")
        initial_info = [election_pb2.NodeInfo(node_id=self.node_id, address=self.my_addr)]
        threading.Thread(
            target=self._forward_election_message,
            args=(self.node_id, [self.node_id], initial_info, 1, self.election_start_time),
            daemon=True
        ).start()
        return True

    def _process_election_step(self, initiator: int, active_nodes: List[int], active_infos: list, hops: int, start_t: float):
        if initiator == self.node_id:
            elapsed_ms = (time.time() - start_t) * 1000.0
            if active_infos:
                best_info = max(active_infos, key=lambda x: x.node_id)
                elected_leader = best_info.node_id
                elected_addr = best_info.address
            else:
                elected_leader = max(active_nodes)
                elected_addr = ""

            self.logger.info(
                f"\n🏆 ==================== ELEIÇÃO CONCLUÍDA ====================\n"
                f"   Volta completa no anel pelo iniciador Nó {self.node_id}!\n"
                f"   Nós ativos: {active_nodes}\n"
                f"   LÍDER ELEITO (Maior ID Aleatório): Nó {elected_leader} em {elected_addr}\n"
                f"   Métricas: Saltos = {hops} | Latência = {elapsed_ms:.2f} ms\n"
                f"============================================================"
            )

            with self.ring_lock:
                self.current_leader = elected_leader
                self.current_leader_address = elected_addr
                self.election_in_progress = False
                self.known_active_nodes = sorted(active_nodes)
                if active_infos:
                    self.known_active_addresses = [info.address for info in active_infos]
                self.missed_heartbeats = 0

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

        with self.ring_lock:
            self.election_in_progress = True
            self.current_leader = None
            if self.node_id not in active_nodes:
                active_nodes.append(self.node_id)
                active_infos.append(election_pb2.NodeInfo(node_id=self.node_id, address=self.my_addr))

        self.logger.info(
            f"➡️ [ENCAMINHANDO ELEIÇÃO] Adicionado Nó {self.node_id} ({self.my_addr}). "
            f"Nós ativos: {active_nodes}. Passando ao próximo no anel..."
        )
        self._forward_election_message(initiator, active_nodes, active_infos, hops + 1, start_t)

    def _forward_election_message(self, initiator: int, active_nodes: List[int], active_infos: list, hops: int, start_t: float):
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
                with grpc.insecure_channel(next_addr) as channel:
                    stub = election_pb2_grpc.RingServiceStub(channel)
                    resp = stub.PassElection(message, timeout=self.rpc_timeout)
                    if resp.accepted:
                        return True
            except grpc.RpcError as e:
                self.logger.warning(
                    f"⚠️ [FALHA NO ANEL] Sucessor {next_addr} inacessível: {e.code()}. "
                    f"PULANDO {next_addr} e tentando o próximo..."
                )

        self.logger.error("❌ Nenhum sucessor no anel respondeu! Elegendo-se a si próprio.")
        with self.ring_lock:
            self.current_leader = self.node_id
            self.current_leader_address = self.my_addr
            self.election_in_progress = False
            self.known_active_nodes = [self.node_id]
        return False

    def _process_coordinator_step(self, leader_id: int, leader_addr: str, announcer_id: int, hops: int, active_nodes: List[int], active_infos: list, start_t: float):
        if announcer_id == self.node_id:
            elapsed_ms = (time.time() - start_t) * 1000.0
            self.logger.info(f"✅ [ANÚNCIO FINALIZADO] Líder Nó {leader_id} consolidado no anel ({elapsed_ms:.2f} ms).")
            return

        with self.ring_lock:
            self.current_leader = leader_id
            self.current_leader_address = leader_addr
            self.election_in_progress = False
            self.known_active_nodes = sorted(active_nodes)
            if active_infos:
                self.known_active_addresses = [info.address for info in active_infos]
            self.missed_heartbeats = 0

        self.logger.info(f"👑 [NOVO LÍDER RECONHECIDO] Nó {self.node_id} reconhece Nó {leader_id} ({leader_addr}) como LÍDER / COORDENADOR 2PC!")
        self._forward_coordinator_message(leader_id, leader_addr, announcer_id, hops + 1, active_nodes, active_infos, start_t)

    def _forward_coordinator_message(self, leader_id: int, leader_addr: str, announcer_id: int, hops: int, active_nodes: List[int], active_infos: list, start_t: float):
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
            except grpc.RpcError:
                pass
        return False

    def _heartbeat_loop(self):
        self.logger.info(f"Monitor de Heartbeat ativo (intervalo={self.heartbeat_interval}s). Aguardando {self.startup_grace_period}s...")
        time.sleep(self.startup_grace_period)

        extracted_idx = "".join([c for c in self.my_addr.split(":")[0] if c.isdigit()])
        idx_num = int(extracted_idx) if extracted_idx else 1
        time.sleep(idx_num * 0.4)

        while self.running:
            time.sleep(self.heartbeat_interval)

            with self.ring_lock:
                leader = self.current_leader
                leader_addr = self.current_leader_address
                in_progress = self.election_in_progress

            if leader is None or not leader_addr:
                if not in_progress:
                    self.logger.info("Nenhum líder configurado após inicialização. Disparando eleição...")
                    self.start_election(reason="boot_without_leader")
                continue

            if leader == self.node_id or leader_addr == self.my_addr:
                continue

            try:
                with grpc.insecure_channel(leader_addr) as channel:
                    stub = election_pb2_grpc.RingServiceStub(channel)
                    stub.Ping(election_pb2.PingRequest(sender_id=self.node_id), timeout=self.heartbeat_timeout)
                    with self.ring_lock:
                        if self.missed_heartbeats > 0:
                            self.logger.info(f"Heartbeat para Líder Nó {leader} restabelecido.")
                        self.missed_heartbeats = 0

                    # Sincroniza estado contábil se o nó acabou de inicializar (Crash-Recovery)
                    if getattr(self, "sync_needed", False):
                        self.sync_bank_state_from_cluster()

            except grpc.RpcError as e:
                with self.ring_lock:
                    self.missed_heartbeats += 1
                    failures = self.missed_heartbeats

                self.logger.warning(
                    f"⚠️ [FALHA DE HEARTBEAT] Líder Nó {leader} ({leader_addr}) inacessível: {e.code()} "
                    f"({failures}/{self.missed_heartbeats_limit})"
                )

                if failures >= self.missed_heartbeats_limit:
                    self.logger.error(
                        f"🚨 [LÍDER MORTO DETECTADO] Líder Nó {leader} falhou {failures} heartbeats consecutivos! "
                        f"Iniciando nova eleição com salto sobre o líder caído..."
                    )
                    with self.ring_lock:
                        self.current_leader = None
                        self.current_leader_address = None
                        self.missed_heartbeats = 0
                    self.start_election(reason=f"leader_{leader}_crashed")

    # =========================================================================
    # SEÇÃO 2: PROTOCOLO 2PC (TWO-PHASE COMMIT) BANCÁRIO
    # =========================================================================

    # -------------------------------------------------------------------------
    # 2.1 Papel de Participante / Coorte (Cohorts)
    # -------------------------------------------------------------------------

    def PrepareTransaction(self, request, context):
        """
        FASE 1 DO 2PC (REQUEST VOTE):
        O participante valida as regras de negócio bancárias:
          - A conta de origem e destino existem?
          - O valor é positivo?
          - As contas estão livres de lock por outra transação pendente?
          - A conta de origem possui saldo suficiente?
        Se válido, adquire o LOCK em memória e responde SIM (vote_commit=True).
        Se inválido, responde NÃO (vote_commit=False) com a justificativa.
        """
        tx_id = request.transaction_id
        src = request.source_account
        dst = request.destination_account
        amt = request.amount

        self.logger.info(
            f"🏦 [2PC COORTE - FASE 1 PREPARE] Recebido Prepare para Tx: {tx_id} | "
            f"Transferência: {src} -> {dst} | Valor: R$ {amt:.2f}"
        )

        # Injeção de Falha (Cenário 3: Crash-stop / Timeout do Participante na Fase 1)
        if self.chaos_hang_prepare:
            self.logger.warning(f"⚠️ [CHAOS SIMULAÇÃO - CENÁRIO 3] Simulando participante travado / crash na Fase 1 para Tx {tx_id}...")
            time.sleep(self.two_pc_timeout + 1.5)

        with self.bank_lock:
            # Idempotência: se já commitou ou abortou anteriormente
            if self.transaction_history.get(tx_id) == "COMMITTED":
                return bank_2pc_pb2.PrepareResponse(
                    transaction_id=tx_id,
                    vote_commit=True,
                    reason="Transação já commitada anteriormente",
                    node_id=self.node_id,
                    node_address=self.my_addr
                )
            if self.transaction_history.get(tx_id) == "ABORTED":
                return bank_2pc_pb2.PrepareResponse(
                    transaction_id=tx_id,
                    vote_commit=False,
                    reason="Transação já abortada anteriormente",
                    node_id=self.node_id,
                    node_address=self.my_addr
                )

            # Validação 1: Contas existem?
            if src not in self.accounts:
                reason = f"Conta de origem '{src}' não existe no livro contábil"
                self.logger.warning(f"❌ [2PC COORTE - VOTO NÃO] {reason}")
                return bank_2pc_pb2.PrepareResponse(
                    transaction_id=tx_id, vote_commit=False, reason=reason,
                    node_id=self.node_id, node_address=self.my_addr
                )

            if dst not in self.accounts:
                reason = f"Conta de destino '{dst}' não existe no livro contábil"
                self.logger.warning(f"❌ [2PC COORTE - VOTO NÃO] {reason}")
                return bank_2pc_pb2.PrepareResponse(
                    transaction_id=tx_id, vote_commit=False, reason=reason,
                    node_id=self.node_id, node_address=self.my_addr
                )

            # Validação 2: Valor positivo?
            if amt <= 0:
                reason = f"Valor da transação inválido: R$ {amt:.2f} (deve ser > 0)"
                self.logger.warning(f"❌ [2PC COORTE - VOTO NÃO] {reason}")
                return bank_2pc_pb2.PrepareResponse(
                    transaction_id=tx_id, vote_commit=False, reason=reason,
                    node_id=self.node_id, node_address=self.my_addr
                )

            # Validação 3: Conflito de Locks (Contas bloqueadas por outra transação pendente)?
            lock_src = self.account_locks.get(src)
            lock_dst = self.account_locks.get(dst)
            if lock_src and lock_src != tx_id:
                reason = f"Conta '{src}' já está bloqueada pela transação pendente {lock_src}"
                self.logger.warning(f"❌ [2PC COORTE - VOTO NÃO] {reason}")
                return bank_2pc_pb2.PrepareResponse(
                    transaction_id=tx_id, vote_commit=False, reason=reason,
                    node_id=self.node_id, node_address=self.my_addr
                )
            if lock_dst and lock_dst != tx_id:
                reason = f"Conta '{dst}' já está bloqueada pela transação pendente {lock_dst}"
                self.logger.warning(f"❌ [2PC COORTE - VOTO NÃO] {reason}")
                return bank_2pc_pb2.PrepareResponse(
                    transaction_id=tx_id, vote_commit=False, reason=reason,
                    node_id=self.node_id, node_address=self.my_addr
                )

            # Validação 4: Saldo suficiente na conta de origem? (Regra de Negócio Bancária)
            current_bal = self.accounts[src]
            if current_bal < amt:
                reason = f"Saldo insuficiente em '{src}': Saldo atual=R$ {current_bal:.2f} < Solicitado=R$ {amt:.2f}"
                self.logger.warning(f"❌ [2PC COORTE - VOTO NÃO] {reason} (Tx: {tx_id})")
                return bank_2pc_pb2.PrepareResponse(
                    transaction_id=tx_id, vote_commit=False, reason=reason,
                    node_id=self.node_id, node_address=self.my_addr
                )

            # SUCESSO: Adquire o LOCK das contas e armazena estado provisório PREPARED
            self.account_locks[src] = tx_id
            self.account_locks[dst] = tx_id
            self.pending_transactions[tx_id] = {
                "source": src,
                "dest": dst,
                "amount": amt,
                "state": "PREPARED",
                "timestamp": time.time(),
            }

            self.logger.info(
                f"🔒 [2PC COORTE - VOTO SIM] Validação aprovada! Locks adquiridos sobre '{src}' e '{dst}'. "
                f"Estado: PREPARED. Respondendo SIM para Coordenador (Tx: {tx_id})."
            )

            return bank_2pc_pb2.PrepareResponse(
                transaction_id=tx_id,
                vote_commit=True,
                reason="OK - Saldo validado e contas bloqueadas",
                node_id=self.node_id,
                node_address=self.my_addr
            )

    def CommitTransaction(self, request, context):
        """
        FASE 2 DO 2PC (COMMIT):
        O participante efetiva as alterações de saldo no banco de dados
        e libera os locks das contas.
        """
        tx_id = request.transaction_id

        with self.bank_lock:
            if tx_id in self.pending_transactions:
                tx = self.pending_transactions.pop(tx_id)
                src = tx["source"]
                dst = tx["dest"]
                amt = tx["amount"]

                # Efetivação contábil atômica
                self.accounts[src] -= amt
                self.accounts[dst] += amt

                # Liberação dos locks
                if self.account_locks.get(src) == tx_id:
                    del self.account_locks[src]
                if self.account_locks.get(dst) == tx_id:
                    del self.account_locks[dst]

                self.transaction_history[tx_id] = "COMMITTED"
                self._save_local_state()

                self.logger.info(
                    f"✅ [2PC COORTE - FASE 2 COMMIT] Transação {tx_id} EFETIVADA! "
                    f"Debitado R$ {amt:.2f} de '{src}' e creditado em '{dst}'. Locks liberados. "
                    f"Novos saldos -> {src}: R$ {self.accounts[src]:.2f} | {dst}: R$ {self.accounts[dst]:.2f}"
                )
                return bank_2pc_pb2.CommitResponse(
                    transaction_id=tx_id,
                    committed=True,
                    message="Commit realizado com sucesso",
                    node_id=self.node_id
                )

            elif self.transaction_history.get(tx_id) == "COMMITTED":
                return bank_2pc_pb2.CommitResponse(
                    transaction_id=tx_id,
                    committed=True,
                    message="Transação já foi commitada (Idempotente)",
                    node_id=self.node_id
                )
            else:
                self.logger.warning(f"⚠️ [2PC COORTE] Commit solicitado para transação desconhecida {tx_id}")
                return bank_2pc_pb2.CommitResponse(
                    transaction_id=tx_id,
                    committed=False,
                    message="Transação não encontrada em estado PREPARED",
                    node_id=self.node_id
                )

    def AbortTransaction(self, request, context):
        """
        FASE 2 DO 2PC (ABORT):
        O participante descarta as alterações provisórias e libera os locks,
        garantindo rollback e mantendo os saldos inalterados.
        """
        tx_id = request.transaction_id
        reason = request.reason or "Comando de Abort do Coordenador"

        with self.bank_lock:
            if tx_id in self.pending_transactions:
                tx = self.pending_transactions.pop(tx_id)
                src = tx["source"]
                dst = tx["dest"]

                # Libera os locks adquiridos na Fase 1
                if self.account_locks.get(src) == tx_id:
                    del self.account_locks[src]
                if self.account_locks.get(dst) == tx_id:
                    del self.account_locks[dst]

                self.transaction_history[tx_id] = "ABORTED"
                self._save_local_state()

                self.logger.info(
                    f"🛑 [2PC COORTE - FASE 2 ABORT] Transação {tx_id} CANCELADA! Motivo: {reason}. "
                    f"Locks sobre '{src}' e '{dst}' LIBERADOS. Saldos preservados."
                )
                return bank_2pc_pb2.AbortResponse(
                    transaction_id=tx_id,
                    aborted=True,
                    message=f"Abort efetuado com sucesso ({reason})",
                    node_id=self.node_id
                )
            else:
                # Remove locks órfãos se existirem
                for acc, l_tx in list(self.account_locks.items()):
                    if l_tx == tx_id:
                        del self.account_locks[acc]
                self.transaction_history[tx_id] = "ABORTED"
                self._save_local_state()
                return bank_2pc_pb2.AbortResponse(
                    transaction_id=tx_id,
                    aborted=True,
                    message="Abort registrado (sem pendências)",
                    node_id=self.node_id
                )

    # -------------------------------------------------------------------------
    # 2.2 Papel de Coordenador (Nó Líder Atual)
    # -------------------------------------------------------------------------

    def SubmitTransaction(self, request, context):
        """
        Ponto de entrada do cliente para submissão de transações bancárias.
        O cliente encaminha a requisição diretamente ao Nó Líder (Coordenador).
        Se este nó não for o líder, ele redireciona ou faz proxy para o líder.
        """
        with self.ring_lock:
            leader_id = self.current_leader
            leader_addr = self.current_leader_address
            is_leader = (leader_id == self.node_id)

        # Se não sou o líder, encaminha a requisição ao líder atual
        if not is_leader:
            if not leader_addr:
                return bank_2pc_pb2.ClientTransactionResponse(
                    transaction_id="",
                    success=False,
                    status="NO_LEADER",
                    message="Nenhum líder eleito no momento. Aguarde a eleição.",
                    coordinator_id=0,
                    coordinator_address="",
                    latency_ms=0.0
                )
            self.logger.info(f"Encaminhando transação ao Líder Atual Nó {leader_id} ({leader_addr})...")
            try:
                with grpc.insecure_channel(leader_addr) as channel:
                    stub = bank_2pc_pb2_grpc.TwoPhaseCommitServiceStub(channel)
                    return stub.SubmitTransaction(request, timeout=self.two_pc_timeout * 3)
            except grpc.RpcError as e:
                return bank_2pc_pb2.ClientTransactionResponse(
                    transaction_id="",
                    success=False,
                    status="LEADER_UNREACHABLE",
                    message=f"Falha ao contatar líder {leader_addr}: {e.code()}",
                    coordinator_id=leader_id or 0,
                    coordinator_address=leader_addr or "",
                    latency_ms=0.0
                )

        # Eu sou o Líder Atual! Executa a coordenação completa do 2PC
        return self._coordinate_2pc_transaction(
            source=request.source_account,
            dest=request.destination_account,
            amount=request.amount,
            test_crash_after_prepare=request.test_crash_leader_after_prepare
        )

    def _coordinate_2pc_transaction(
        self,
        source: str,
        dest: str,
        amount: float,
        test_crash_after_prepare: bool = False
    ) -> bank_2pc_pb2.ClientTransactionResponse:
        """
        Coordenação do 2PC executada exclusivamente pelo Líder Atual:
          - Fase 1: Envia PrepareTransaction para todas as réplicas ativas.
          - Coleta de Votos: Tratamento explícito de timeout gRPC como voto "NÃO".
          - Fase 2: Se 100% SIM -> CommitTransaction para todas.
                    Se >= 1 NÃO ou timeout -> AbortTransaction para todas.
        """
        tx_id = f"tx_{uuid.uuid4().hex[:8]}"
        t_start = time.time()

        self.logger.info(
            f"\n{'='*75}\n"
            f"👑 [2PC COORDENADOR - INÍCIO] Nova Transação Bancária: {tx_id}\n"
            f"   Operação: Debitar R$ {amount:.2f} de '{source}' e Creditar em '{dest}'\n"
            f"   Coordenador: Nó {self.node_id} ({self.my_addr})\n"
            f"{'='*75}"
        )

        # Identificação dos participantes da transação 2PC
        with self.ring_lock:
            active_addrs = list(self.known_active_addresses) if self.known_active_addresses else []
            ring_addrs = list(self.ring_addresses)

        # Se conhecemos nós ativos confirmados pela eleição em anel, usamos os nós ativos;
        # caso contrário, usamos a lista de topologia configurada.
        if active_addrs and len(active_addrs) >= 2:
            participants = list(active_addrs)
            if self.my_addr not in participants:
                participants.append(self.my_addr)
        else:
            participants = ring_addrs

        remote_participants = [addr for addr in participants if addr != self.my_addr]

        tx_data = bank_2pc_pb2.TransactionData(
            transaction_id=tx_id,
            source_account=source,
            destination_account=dest,
            amount=amount,
            sender_node=self.my_addr,
            timestamp=t_start
        )

        # -------------------------------------------------------------
        # FASE 1: PREPARE (REQUEST VOTE)
        # -------------------------------------------------------------
        self.logger.info(f"📢 [2PC COORDENADOR - FASE 1] Solicitando votos para {len(participants)} nós...")
        votes: Dict[str, bool] = {}
        abort_reasons: List[str] = []

        # 1.1 Voto Local do próprio Coordenador
        local_prep = self.PrepareTransaction(tx_data, None)
        if local_prep.vote_commit:
            votes[self.my_addr] = True
            self.logger.info(f"   📥 [Voto Local] Coordenador Nó {self.node_id}: SIM")
        else:
            votes[self.my_addr] = False
            abort_reasons.append(f"Líder ({self.my_addr}): {local_prep.reason}")
            self.logger.warning(f"   📥 [Voto Local] Coordenador Nó {self.node_id}: NÃO ({local_prep.reason})")

        # 1.2 Votos Remotos das Coortes (com deadline explícito e concorrência)
        def query_cohort(addr: str) -> Tuple[str, bool, str]:
            try:
                with grpc.insecure_channel(addr) as channel:
                    stub = bank_2pc_pb2_grpc.TwoPhaseCommitServiceStub(channel)
                    resp = stub.PrepareTransaction(tx_data, timeout=self.two_pc_timeout)
                    return addr, resp.vote_commit, resp.reason
            except grpc.RpcError as rpc_err:
                # TRATAMENTO DE TIMEOUT / QUEDA (Requisito Formal do Trabalho):
                # Se algum nó não responder dentro do tempo estipulado ou se a rede falhar,
                # o Coordenador computa como voto "NÃO".
                err_desc = f"Timeout/Falha gRPC ({rpc_err.code().name if hasattr(rpc_err, 'code') else rpc_err})"
                return addr, False, err_desc
            except Exception as e:
                return addr, False, f"Exceção inesperada: {str(e)}"

        if remote_participants:
            with futures.ThreadPoolExecutor(max_workers=len(remote_participants)) as executor:
                future_map = {executor.submit(query_cohort, addr): addr for addr in remote_participants}
                for fut in futures.as_completed(future_map):
                    addr, vote, reason = fut.result()
                    votes[addr] = vote
                    if vote:
                        self.logger.info(f"   📥 [Voto Coorte] {addr}: SIM")
                    else:
                        self.logger.warning(f"   📥 [Voto Coorte] {addr}: NÃO ({reason})")
                        abort_reasons.append(f"{addr}: {reason}")

        all_yes = all(votes.values()) and len(votes) == len(participants)

        # -------------------------------------------------------------
        # INJEÇÃO DE FALHA CRÍTICA (CENÁRIO 4: CRASH DO LÍDER PÓS-FASE 1)
        # -------------------------------------------------------------
        if all_yes and (test_crash_after_prepare or self.chaos_crash_after_prepare):
            self.logger.critical(
                f"\n💥💥💥 [CHAOS TEST - CENÁRIO 4] CRASH-STOP IMEDIATO DO LÍDER PÓS-FASE 1! 💥💥💥\n"
                f"   Todos os nós votaram SIM para a transação {tx_id}.\n"
                f"   Coortes estão em PREPARED retendo LOCKS nos saldos bancários.\n"
                f"   O Líder colapsa ANTES de emitir o comando de Commit da Fase 2.\n"
                f"   Comprovando o PROBLEMA DO BLOQUEIO (BLOCKING PROBLEM) do 2PC...\n"
                f"{'='*75}"
            )
            os._exit(42)

        # -------------------------------------------------------------
        # FASE 2: DECISÃO GLOBAL (COMMIT ou ABORT)
        # -------------------------------------------------------------
        tx_req = bank_2pc_pb2.TransactionID(
            transaction_id=tx_id,
            reason="; ".join(abort_reasons) if abort_reasons else "Votação unânime confirmada"
        )

        if all_yes:
            # 100% SIM -> COMMIT GLOBAL
            self.logger.info(
                f"✨ [2PC COORDENADOR - FASE 2 COMMIT] Votação unânime de todos os {len(participants)} nós! "
                f"Transmitindo CommitTransaction para todas as réplicas..."
            )
            # Commit local
            self.CommitTransaction(tx_req, None)

            def broadcast_commit(addr: str):
                try:
                    with grpc.insecure_channel(addr) as channel:
                        stub = bank_2pc_pb2_grpc.TwoPhaseCommitServiceStub(channel)
                        stub.CommitTransaction(tx_req, timeout=self.two_pc_timeout)
                except Exception as e:
                    self.logger.warning(f"Falha ao notificar Commit para {addr}: {e}")

            if remote_participants:
                with futures.ThreadPoolExecutor(max_workers=len(remote_participants)) as executor:
                    list(executor.map(broadcast_commit, remote_participants))

            latency_ms = (time.time() - t_start) * 1000.0
            self.logger.info(f"🏆 [2PC FINALIZADO] Transação {tx_id} EFETIVADA com sucesso em {latency_ms:.2f} ms.\n")

            return bank_2pc_pb2.ClientTransactionResponse(
                transaction_id=tx_id,
                success=True,
                status="COMMITTED",
                message="Transação bancária commitada com sucesso em todas as réplicas.",
                coordinator_id=self.node_id,
                coordinator_address=self.my_addr,
                latency_ms=latency_ms
            )

        else:
            # >= 1 NÃO ou TIMEOUT -> ABORT GLOBAL
            self.logger.warning(
                f"🛑 [2PC COORDENADOR - FASE 2 ABORT] Transação {tx_id} REJEITADA ({len(abort_reasons)} falha(s)). "
                f"Motivos: {abort_reasons}. Transmitindo AbortTransaction para as réplicas..."
            )
            # Abort local
            self.AbortTransaction(tx_req, None)

            def broadcast_abort(addr: str):
                try:
                    with grpc.insecure_channel(addr) as channel:
                        stub = bank_2pc_pb2_grpc.TwoPhaseCommitServiceStub(channel)
                        stub.AbortTransaction(tx_req, timeout=self.two_pc_timeout)
                except Exception as e:
                    self.logger.warning(f"Falha ao notificar Abort para {addr}: {e}")

            if remote_participants:
                with futures.ThreadPoolExecutor(max_workers=len(remote_participants)) as executor:
                    list(executor.map(broadcast_abort, remote_participants))

            latency_ms = (time.time() - t_start) * 1000.0
            self.logger.warning(f"❌ [2PC FINALIZADO] Transação {tx_id} ABORTADA em {latency_ms:.2f} ms. Rollback completo.\n")

            return bank_2pc_pb2.ClientTransactionResponse(
                transaction_id=tx_id,
                success=False,
                status="ABORTED",
                message=f"Transação abortada: {'; '.join(abort_reasons)}",
                coordinator_id=self.node_id,
                coordinator_address=self.my_addr,
                latency_ms=latency_ms
            )

    # -------------------------------------------------------------------------
    # 2.3 Diagnóstico, Recuperação de Bloqueio e Chaos Engineering
    # -------------------------------------------------------------------------

    def GetBankState(self, request, context):
        """
        Retorna o estado detalhado do banco de dados neste nó:
          - Saldos de todas as contas
          - Situação dos locks (se a conta está bloqueada e por qual transação)
          - Transações pendentes em estado PREPARED
        """
        with self.bank_lock:
            acc_list = []
            for acc_name, balance in sorted(self.accounts.items()):
                tx_lock = self.account_locks.get(acc_name, "")
                acc_list.append(bank_2pc_pb2.AccountInfo(
                    account_id=acc_name,
                    balance=balance,
                    is_locked=bool(tx_lock),
                    locked_by_tx=tx_lock
                ))

            pending_ids = list(self.pending_transactions.keys())
            return bank_2pc_pb2.BankStateResponse(
                node_id=self.node_id,
                node_address=self.my_addr,
                accounts=acc_list,
                pending_transactions_count=len(pending_ids),
                pending_tx_ids=pending_ids
            )

    def ResolveBlockedTransaction(self, request, context):
        """
        Mecanismo de Recuperação pós-Eleição de Novo Líder (Cenário 4):
        Quando o Coordenador anterior morre e os participantes ficam bloqueados
        no estado PREPARED, o novo líder eleito pode ser acionado para resolver
        a transação pendente (enviando Abort global para liberar os locks).
        """
        tx_id = request.transaction_id
        action = request.action.upper() or "ABORT"

        self.logger.info(
            f"🛠️ [RECUPERAÇÃO DE BLOQUEIO 2PC] Novo Coordenador Nó {self.node_id} resolvendo "
            f"transação bloqueada '{tx_id}' com ação '{action}'..."
        )

        # Se tx_id for vazio ou "ALL", busca todas as transações pendentes ou com lock
        tx_ids = []
        if tx_id and tx_id != "ALL":
            tx_ids = [tx_id]
        else:
            with self.bank_lock:
                tx_ids = list(self.pending_transactions.keys())
                for l_tx in self.account_locks.values():
                    if l_tx and l_tx not in tx_ids:
                        tx_ids.append(l_tx)

        if not tx_ids and tx_id:
            tx_ids = [tx_id]

        for tid in tx_ids:
            tx_req = bank_2pc_pb2.TransactionID(
                transaction_id=tid,
                reason=f"Resolução de bloqueio pós-queda de coordenador via Novo Líder Nó {self.node_id}"
            )

            # Executa localmente no próprio nó
            if action == "COMMIT":
                self.CommitTransaction(tx_req, None)
            else:
                self.AbortTransaction(tx_req, None)

            # Envia para todos os outros nós ativos do anel
            for addr in self.ring_addresses:
                if addr == self.my_addr:
                    continue
                try:
                    with grpc.insecure_channel(addr) as channel:
                        stub = bank_2pc_pb2_grpc.TwoPhaseCommitServiceStub(channel)
                        if action == "COMMIT":
                            stub.CommitTransaction(tx_req, timeout=self.two_pc_timeout)
                        else:
                            stub.AbortTransaction(tx_req, timeout=self.two_pc_timeout)
                except Exception as e:
                    self.logger.warning(f"Aviso durante resolução de {tid} em {addr}: {e}")

        return bank_2pc_pb2.ResolveResponse(
            transaction_id=tx_id or "ALL",
            resolved=True,
            message=f"Transação(ões) {tx_ids or [tx_id]} resolvida(s) como {action} em todos os nós sobreviventes."
        )

    def SetChaosHook(self, request, context):
        self.chaos_crash_after_prepare = request.crash_after_prepare
        self.chaos_hang_prepare = request.hang_prepare
        if request.crash_node:
            self.logger.critical(f"💥 [CHAOS HOOK] Comando de crash-stop recebido! Finalizando processo do Nó {self.node_id}...")
            threading.Thread(target=lambda: (time.sleep(0.1), os._exit(99))).start()
        msg = f"Chaos Hook atualizado: crash_after_prep={self.chaos_crash_after_prepare}, hang_prep={self.chaos_hang_prepare}"
        self.logger.warning(f"⚠️ [CHAOS HOOK] {msg}")
        return bank_2pc_pb2.ChaosResponse(ack=True, message=msg)

    # -------------------------------------------------------------------------
    # 2.4 Persistência Local e Recuperação de Falhas (Crash-Recovery)
    # -------------------------------------------------------------------------

    def _save_local_state(self):
        """Persiste o estado do banco em disco local (Crash-Recovery / WAL)."""
        try:
            state_file = f"/tmp/bank_state_{self.my_addr.split(':')[0]}.json"
            with open(state_file, "w") as f:
                json.dump({
                    "accounts": self.accounts,
                    "history": self.transaction_history
                }, f)
        except Exception as e:
            self.logger.warning(f"Aviso ao salvar estado local: {e}")

    def _load_local_state(self) -> bool:
        """Carrega o estado contábil persistido após reinicialização do nó."""
        state_file = f"/tmp/bank_state_{self.my_addr.split(':')[0]}.json"
        if os.path.exists(state_file):
            try:
                with open(state_file, "r") as f:
                    data = json.load(f)
                    if "accounts" in data:
                        self.accounts = data["accounts"]
                    if "history" in data:
                        self.transaction_history = data["history"]
                self.logger.info(f"💾 [CRASH-RECOVERY] Estado local restaurado do disco: {self.accounts}")
                return True
            except Exception as e:
                self.logger.warning(f"Aviso ao carregar estado local: {e}")
        return False

    def sync_bank_state_from_cluster(self) -> bool:
        """
        Sincroniza os saldos bancários com o líder ou nós ativos sobreviventes.
        Garante consistência imediata quando um nó reinicia (Crash-Recovery).
        """
        targets = []
        with self.ring_lock:
            if self.current_leader_address and self.current_leader_address != self.my_addr:
                targets.append(self.current_leader_address)
            targets.extend([a for a in self.ring_addresses if a != self.my_addr and a not in targets])

        for addr in targets:
            try:
                with grpc.insecure_channel(addr) as channel:
                    stub = bank_2pc_pb2_grpc.TwoPhaseCommitServiceStub(channel)
                    st = stub.GetBankState(bank_2pc_pb2.BankStateRequest(), timeout=1.5)
                    if st.accounts:
                        with self.bank_lock:
                            for acc in st.accounts:
                                self.accounts[acc.account_id] = acc.balance
                            # Limpa locks órfãos locais que não estejam pendentes no nó de referência
                            active_txs = set(st.pending_tx_ids)
                            for acc_name, l_tx in list(self.account_locks.items()):
                                if l_tx not in active_txs:
                                    self.logger.info(f"🧹 [SYNC] Liberando lock órfão da conta '{acc_name}' (Tx {l_tx})")
                                    del self.account_locks[acc_name]
                                    if l_tx in self.pending_transactions:
                                        del self.pending_transactions[l_tx]
                            self._save_local_state()
                        self.logger.info(
                            f"🔄 [CRASH-RECOVERY] Saldos sincronizados com sucesso a partir de {addr}: "
                            f"{self.accounts}"
                        )
                        self.sync_needed = False
                        return True
            except Exception:
                continue
        return False

    def SyncBankState(self, request, context):
        """Endpoint gRPC para forçar sincronização de saldos sob demanda."""
        self.sync_bank_state_from_cluster()
        return self.GetBankState(request, context)

    # =========================================================================
    # Inicialização do Servidor gRPC
    # =========================================================================

    def serve(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
        # Registra ambos os serviços no mesmo servidor gRPC
        election_pb2_grpc.add_RingServiceServicer_to_server(self, server)
        bank_2pc_pb2_grpc.add_TwoPhaseCommitServiceServicer_to_server(self, server)

        bind_addr = f"0.0.0.0:{self.port}"
        server.add_insecure_port(bind_addr)
        server.start()
        self.logger.info(f"🚀 Servidor gRPC ativo escutando em {bind_addr}...")

        hb_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        hb_thread.start()

        try:
            server.wait_for_termination()
        except KeyboardInterrupt:
            self.logger.info("Encerrando nó...")
            self.running = False
            server.stop(0)

if __name__ == "__main__":
    node = DistributedBankNode()
    node.serve()
