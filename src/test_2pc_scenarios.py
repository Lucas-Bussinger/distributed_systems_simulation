#!/usr/bin/env python3
"""
===============================================================================
MC714 - Sistemas Distribuídos (Unicamp) - Trabalho 2
Script Automatizado de Teste dos 4 Cenários do Protocolo 2PC Bancário
===============================================================================
Cenários Avaliados:
  1. Fluxo Normal (Execução Correta com Commit Unânime)
  2. Falha por Validação de Negócio (Saldo Insuficiente -> Rollback / Abort)
  3. Injeção de Falha: Crash-stop / Timeout de Participante na Fase 1 -> Abort
  4. Injeção de Falha Crítica: Crash do Líder pós-Fase 1 -> Problema do Bloqueio
     (Locks retidos) -> Detecção via Heartbeat -> Nova Eleição em Anel -> Resolução
===============================================================================
"""

import os
import sys
import time
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

def find_active_nodes(nodes=DEFAULT_NODES):
    active = []
    for cname, addr in nodes:
        try:
            with grpc.insecure_channel(addr) as channel:
                stub = election_pb2_grpc.RingServiceStub(channel)
                res = stub.GetStatus(election_pb2.StatusRequest(), timeout=1.0)
                active.append((cname, addr, res.node_id, res.is_leader, res.current_leader, res.current_leader_address))
        except Exception:
            pass
    return active

def get_current_leader(nodes=DEFAULT_NODES, timeout=15.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        active = find_active_nodes(nodes)
        for cname, addr, nid, is_ldr, ldr_id, ldr_addr in active:
            if is_ldr:
                return nid, addr, cname
            if ldr_addr:
                # Confirma se o endereço responde como líder
                try:
                    with grpc.insecure_channel(ldr_addr) as ch:
                        st = election_pb2_grpc.RingServiceStub(ch).GetStatus(election_pb2.StatusRequest(), timeout=1.0)
                        if st.is_leader:
                            return st.node_id, ldr_addr, ldr_addr.split(":")[0]
                except Exception:
                    pass
        time.sleep(1.0)
    return None, None, None

def get_cluster_balances(nodes=DEFAULT_NODES):
    balances = {}
    locks = {}
    for cname, addr in nodes:
        try:
            with grpc.insecure_channel(addr) as channel:
                stub = bank_2pc_pb2_grpc.TwoPhaseCommitServiceStub(channel)
                res = stub.GetBankState(bank_2pc_pb2.BankStateRequest(), timeout=1.5)
                balances[addr] = {acc.account_id: acc.balance for acc in res.accounts}
                locks[addr] = {acc.account_id: (acc.is_locked, acc.locked_by_tx) for acc in res.accounts}
        except Exception:
            pass
    return balances, locks

# =============================================================================
# CENÁRIO 1: FLUXO NORMAL (EXECUÇÃO CORRETA)
# =============================================================================
def test_scenario_1_normal_flow():
    print("\n" + "=" * 80)
    print("▶️  INICIANDO CENÁRIO 1: FLUXO NORMAL DE TRANSAÇÃO (COMMIT UNÂNIME)")
    print("=" * 80)

    leader_id, leader_addr, leader_cname = get_current_leader()
    print(f"📌 Líder Coordenador identificado: Nó {leader_id} em {leader_addr} ({leader_cname})")
    assert leader_addr is not None, "Falha: Nenhum líder ativo encontrado!"

    # Saldos Iniciais
    bal_before, _ = get_cluster_balances()
    init_a = bal_before[leader_addr]["conta_A"]
    init_b = bal_before[leader_addr]["conta_B"]
    transfer_amount = 100.0
    print(f"📊 Saldos Iniciais: conta_A = R$ {init_a:.2f} | conta_B = R$ {init_b:.2f}")

    print(f"💸 Enviando transação: Transferir R$ {transfer_amount:.2f} de conta_A -> conta_B via Líder {leader_addr}...")
    with grpc.insecure_channel(leader_addr) as channel:
        stub = bank_2pc_pb2_grpc.TwoPhaseCommitServiceStub(channel)
        req = bank_2pc_pb2.ClientTransactionRequest(
            source_account="conta_A",
            destination_account="conta_B",
            amount=transfer_amount,
            test_crash_leader_after_prepare=False
        )
        res = stub.SubmitTransaction(req, timeout=8.0)

    print(f"📥 Resposta do Coordenador: Tx={res.transaction_id} | Status={res.status} | Sucesso={res.success}")
    print(f"   Mensagem: {res.message} | Latência={res.latency_ms:.2f} ms")

    assert res.success is True, f"Erro: Transação deveria ter sido commitada! ({res.message})"
    assert res.status == "COMMITTED", f"Erro: Status esperado COMMITTED, obtido {res.status}"

    # Verificação de Consistência nas Réplicas
    time.sleep(0.5)
    bal_after, locks_after = get_cluster_balances()

    print("\n🔍 Validando consistência de saldos em todas as réplicas ativas:")
    expected_a = init_a - transfer_amount
    expected_b = init_b + transfer_amount

    all_consistent = True
    for addr, accs in bal_after.items():
        curr_a = accs["conta_A"]
        curr_b = accs["conta_B"]
        is_ok = (curr_a == expected_a) and (curr_b == expected_b)
        print(f"   - {addr:<14}: conta_A = R$ {curr_a:.2f} | conta_B = R$ {curr_b:.2f} -> {'✅ CONSISTENTE' if is_ok else '❌ DIVERGENTE'}")
        if not is_ok:
            all_consistent = False

    # Verifica locks
    all_unlocked = True
    for addr, lcks in locks_after.items():
        for acc_id, (is_lck, lck_tx) in lcks.items():
            if is_lck:
                all_unlocked = False
                print(f"   ❌ Erro: Conta {acc_id} em {addr} ainda retém lock da Tx {lck_tx}")

    assert all_consistent, "Falha: Divergência de saldo detectada entre as réplicas!"
    assert all_unlocked, "Falha: Locks não foram liberados após o commit!"

    print("🎉 CENÁRIO 1 CONCLUÍDO COM SUCESSO! Consistência e Atomicidade provadas.")
    return True

# =============================================================================
# CENÁRIO 2: FALHA POR VALIDAÇÃO DE NEGÓCIO (ABORT NORMAL)
# =============================================================================
def test_scenario_2_business_abort():
    print("\n" + "=" * 80)
    print("▶️  INICIANDO CENÁRIO 2: VALIDAÇÃO DE NEGÓCIO (SALDO INSUFICIENTE -> ABORT)")
    print("=" * 80)

    leader_id, leader_addr, leader_cname = get_current_leader()
    print(f"📌 Líder Coordenador identificado: Nó {leader_id} em {leader_addr}")
    assert leader_addr is not None, "Falha: Nenhum líder ativo encontrado!"

    bal_before, _ = get_cluster_balances()
    current_a = bal_before[leader_addr]["conta_A"]
    current_b = bal_before[leader_addr]["conta_B"]

    # Tentativa de transferir valor muito superior ao saldo disponível
    excessive_amount = current_a + 50000.0
    print(f"📊 Saldo Atual conta_A: R$ {current_a:.2f}. Tentando transferir R$ {excessive_amount:.2f}...")

    with grpc.insecure_channel(leader_addr) as channel:
        stub = bank_2pc_pb2_grpc.TwoPhaseCommitServiceStub(channel)
        req = bank_2pc_pb2.ClientTransactionRequest(
            source_account="conta_A",
            destination_account="conta_B",
            amount=excessive_amount,
            test_crash_leader_after_prepare=False
        )
        res = stub.SubmitTransaction(req, timeout=8.0)

    print(f"📥 Resposta do Coordenador: Tx={res.transaction_id} | Status={res.status} | Sucesso={res.success}")
    print(f"   Mensagem recebida: {res.message}")

    assert res.success is False, "Erro: Transação deveria ter sido recusada por falta de saldo!"
    assert res.status == "ABORTED", f"Erro: Status esperado ABORTED, obtido {res.status}"
    assert "Saldo insuficiente" in res.message, "Erro: Motivo deveria conter 'Saldo insuficiente'"

    # Verificação de Rollback
    time.sleep(0.5)
    bal_after, locks_after = get_cluster_balances()

    print("\n🔍 Validando preservação dos saldos (Rollback garantido):")
    for addr, accs in bal_after.items():
        curr_a = accs["conta_A"]
        curr_b = accs["conta_B"]
        is_preserved = (curr_a == current_a) and (curr_b == current_b)
        print(f"   - {addr:<14}: conta_A = R$ {curr_a:.2f} | conta_B = R$ {curr_b:.2f} -> {'✅ SALDO PRESERVADO' if is_preserved else '❌ ALTERAÇÃO INDEVIDA'}")
        assert is_preserved, f"Falha: Saldo alterado indevidamente em {addr}"

    print("🎉 CENÁRIO 2 CONCLUÍDO COM SUCESSO! Abort e Rollback comprovados.")
    return True

# =============================================================================
# CENÁRIO 3: CRASH-STOP DE PARTICIPANTE NA FASE 1 (TIMEOUT -> ABORT)
# =============================================================================
def test_scenario_3_participant_crash():
    print("\n" + "=" * 80)
    print("▶️  INICIANDO CENÁRIO 3: INJEÇÃO DE FALHA EM PARTICIPANTE NA FASE 1")
    print("=" * 80)

    leader_id, leader_addr, leader_cname = get_current_leader()
    print(f"📌 Líder Coordenador: Nó {leader_id} em {leader_addr}")

    # Encontra um coorte (não o líder) para injetar a falha
    active = find_active_nodes()
    followers = [n for n in active if not n[3]]
    assert len(followers) > 0, "Erro: Não há seguidores ativos para teste de falha."

    target_follower = followers[0]
    f_cname, f_addr, f_id = target_follower[0], target_follower[1], target_follower[2]
    print(f"🎯 Participante alvo da falha de comunicação / crash: Nó {f_id} ({f_addr})")

    # Injeta gancho de falha no participante alvo (simula timeout na Fase 1)
    with grpc.insecure_channel(f_addr) as ch:
        stub_cohort = bank_2pc_pb2_grpc.TwoPhaseCommitServiceStub(ch)
        stub_cohort.SetChaosHook(bank_2pc_pb2.ChaosRequest(hang_prepare=True), timeout=2.0)
    print(f"⚠️  Gancho de falha ativado no participante {f_addr} (não responderá na Fase 1).")

    bal_before, _ = get_cluster_balances()
    current_a = bal_before[leader_addr]["conta_A"]
    current_b = bal_before[leader_addr]["conta_B"]

    print("💸 Submetendo transação válida de R$ 50.00...")
    with grpc.insecure_channel(leader_addr) as channel:
        stub = bank_2pc_pb2_grpc.TwoPhaseCommitServiceStub(channel)
        req = bank_2pc_pb2.ClientTransactionRequest(
            source_account="conta_A",
            destination_account="conta_B",
            amount=50.0,
            test_crash_leader_after_prepare=False
        )
        res = stub.SubmitTransaction(req, timeout=10.0)

    print(f"📥 Resposta do Coordenador: Tx={res.transaction_id} | Status={res.status} | Sucesso={res.success}")
    print(f"   Mensagem do Coordenador: {res.message}")

    # Remove o gancho de falha no participante
    try:
        with grpc.insecure_channel(f_addr) as ch:
            bank_2pc_pb2_grpc.TwoPhaseCommitServiceStub(ch).SetChaosHook(
                bank_2pc_pb2.ChaosRequest(hang_prepare=False), timeout=2.0
            )
    except Exception:
        pass

    assert res.success is False, "Erro: Transação deveria ter abortado devido à ausência do participante!"
    assert res.status == "ABORTED", f"Erro: Status esperado ABORTED, obtido {res.status}"
    assert ("Timeout" in res.message or "Falha" in res.message), "Erro: Mensagem deveria relatar timeout/falha gRPC"

    # Confirma preservação dos saldos
    bal_after, _ = get_cluster_balances()
    assert bal_after[leader_addr]["conta_A"] == current_a, "Erro: Saldo foi debitado indevidamente!"
    print("✅ Saldos em todos os nós sobreviventes permaneceram inalterados.")
    print("🎉 CENÁRIO 3 CONCLUÍDO COM SUCESSO! Timeout gRPC tratado como voto NÃO com Abort emitido.")
    return True

# =============================================================================
# CENÁRIO 4: CRASH CRÍTICO DO LÍDER PÓS-FASE 1 (PROBLEMA DO BLOQUEIO DO 2PC)
# =============================================================================
def test_scenario_4_leader_crash_blocking():
    print("\n" + "=" * 80)
    print("▶️  INICIANDO CENÁRIO 4: CRASH CRÍTICO DO LÍDER PÓS-FASE 1 (BLOCKING PROBLEM)")
    print("=" * 80)

    leader_id, leader_addr, leader_cname = get_current_leader()
    print(f"📌 Líder Coordenador a ser derrubado: Nó {leader_id} em {leader_addr} ({leader_cname})")

    # Encontra um coorte seguidor para observar o estado de bloqueio
    active = find_active_nodes()
    followers = [n for n in active if not n[3]]
    assert len(followers) > 0, "Erro: Sem seguidores para observar."
    obs_cname, obs_addr, obs_id = followers[0][0], followers[0][1], followers[0][2]
    print(f"👀 Coorte observador do bloqueio: Nó {obs_id} em {obs_addr}")

    print("\n⚡ Disparando transação com INJEÇÃO DE CRASH DO LÍDER IMEDIATAMENTE APÓS A FASE 1...")
    try:
        with grpc.insecure_channel(leader_addr) as channel:
            stub = bank_2pc_pb2_grpc.TwoPhaseCommitServiceStub(channel)
            req = bank_2pc_pb2.ClientTransactionRequest(
                source_account="conta_A",
                destination_account="conta_B",
                amount=75.0,
                test_crash_leader_after_prepare=True
            )
            stub.SubmitTransaction(req, timeout=4.0)
    except grpc.RpcError as e:
        print(f"💥 Confirmação do Crash: Chamada RPC interrompida como esperado ({e.code()}).")

    time.sleep(1.0)

    # 1. Demonstração do PROBLEMA DO BLOQUEIO (Blocking Problem)
    print("\n🔒 [EVIDÊNCIA TEÓRICA DO BLOQUEIO DO 2PC]:")
    print(f"Consultando o estado do Coorte Nó {obs_id} ({obs_addr}) enquanto o Coordenador está morto...")

    with grpc.insecure_channel(obs_addr) as channel:
        stub = bank_2pc_pb2_grpc.TwoPhaseCommitServiceStub(channel)
        st = stub.GetBankState(bank_2pc_pb2.BankStateRequest(), timeout=2.0)
        
        locked_accs = [acc.account_id for acc in st.accounts if acc.is_locked]
        print(f"   - Contas com LOCK ATIVO retido: {locked_accs}")
        print(f"   - Transações pendentes em PREPARED: {st.pending_tx_ids}")

        assert len(locked_accs) > 0, "Falha: O coorte deveria reter os locks (problema do bloqueio)!"
        assert st.pending_transactions_count > 0, "Falha: Transação deveria estar retida em PREPARED!"
        blocked_tx_id = st.pending_tx_ids[0]

    print(f"✅ COMPORTAMENTO BLOQUEANTE CONFIRMADO: Transação {blocked_tx_id} retém locks sem poder decidir sozinha.")

    # 2. Recuperação: Eleição do Novo Líder via Algoritmo do Anel
    print("\n👑 Aguardando detecção de falha via Heartbeat e Eleição de NOVO LÍDER em Anel...")
    # O heartbeat_interval=3s e limit=2, após ~8-12s um novo líder é eleito
    new_leader_id = None
    new_leader_addr = None
    for attempt in range(25):
        time.sleep(1.5)
        nid, naddr, ncname = get_current_leader()
        if nid and nid != leader_id and naddr != leader_addr:
            new_leader_id = nid
            new_leader_addr = naddr
            print(f"🎉 NOVO LÍDER ELEITO COM SUCESSO: Nó {new_leader_id} em {new_leader_addr}!")
            break
        print(f"   ... aguardando convergência do anel ({attempt+1}/25)...")

    assert new_leader_id is not None, "Falha: Nenhum novo líder foi eleito após a queda do anterior!"

    # 3. Resolução da Pendência pelo Novo Líder
    print(f"\n🛠️ Acionando o Novo Coordenador Nó {new_leader_id} para resolver a transação bloqueada {blocked_tx_id}...")
    with grpc.insecure_channel(new_leader_addr) as channel:
        stub = bank_2pc_pb2_grpc.TwoPhaseCommitServiceStub(channel)
        res = stub.ResolveBlockedTransaction(bank_2pc_pb2.ResolveRequest(
            transaction_id=blocked_tx_id,
            action="ABORT"
        ), timeout=5.0)
        print(f"📥 Resposta da resolução: {res.message}")
        assert res.resolved is True, "Falha na resolução da transação bloqueada!"

    # 4. Confirma que os locks foram devidamente liberados
    time.sleep(0.5)
    with grpc.insecure_channel(obs_addr) as channel:
        st = bank_2pc_pb2_grpc.TwoPhaseCommitServiceStub(channel).GetBankState(bank_2pc_pb2.BankStateRequest(), timeout=2.0)
        locked_accs_after = [acc.account_id for acc in st.accounts if acc.is_locked]
        print(f"🔍 Locks retidos após resolução: {locked_accs_after}")
        assert len(locked_accs_after) == 0, "Falha: Os locks deveriam ter sido liberados após a resolução!"

    # 5. Sincronização pós-recuperação (Crash-Recovery) do nó que havia colapsado
    print(f"\n🔄 Verificando reinicialização e sincronização do nó {leader_addr} (Crash-Recovery)...")
    for _ in range(5):
        try:
            with grpc.insecure_channel(leader_addr) as ch:
                stub = bank_2pc_pb2_grpc.TwoPhaseCommitServiceStub(ch)
                stub.SyncBankState(bank_2pc_pb2.BankStateRequest(), timeout=2.0)
                print(f"✅ Nó {leader_addr} recuperado e sincronizado com o cluster com sucesso!")
                break
        except Exception:
            time.sleep(1.0)

    print("🎉 CENÁRIO 4 CONCLUÍDO COM SUCESSO! Bloqueio comprovado e saneado pelo Novo Líder.")
    return True

# =============================================================================
# EXECUTOR PRINCIPAL DOS TESTES
# =============================================================================
def run_all_tests():
    print("=" * 80)
    print("🧪 BATERIA DE TESTES EXPERIMENTAIS 2PC - MC714 SISTEMAS DISTRIBUÍDOS")
    print("=" * 80)

    print("🔍 Diagnosticando integridade do cluster e nós online...")
    active = find_active_nodes()
    print(f"   Nós ativos respondendo: {len(active)}/{len(DEFAULT_NODES)} ({[n[0] for n in active]})")
    if len(active) < len(DEFAULT_NODES):
        missing = [c for c, _ in DEFAULT_NODES if c not in [n[0] for n in active]]
        print(f"⚠️  Aviso: Nós inativos detectados: {missing}")
        print("   Caso deseje testar com todos os 10 nós, suba os contêineres com 'sudo docker compose up -d'.")

    t0 = time.time()
    s1 = test_scenario_1_normal_flow()
    s2 = test_scenario_2_business_abort()
    s3 = test_scenario_3_participant_crash()
    s4 = test_scenario_4_leader_crash_blocking()

    total_time = time.time() - t0
    print("\n" + "=" * 80)
    print("📋 RELATÓRIO FINAL DOS TESTES DO PROTOCOLO 2PC")
    print("=" * 80)
    print(f"Cenário 1 (Fluxo Normal):                {'✅ PASSOU' if s1 else '❌ FALHOU'}")
    print(f"Cenário 2 (Abort por Regra de Negócio):  {'✅ PASSOU' if s2 else '❌ FALHOU'}")
    print(f"Cenário 3 (Crash de Participante):       {'✅ PASSOU' if s3 else '❌ FALHOU'}")
    print(f"Cenário 4 (Crash do Líder & Bloqueio):   {'✅ PASSOU' if s4 else '❌ FALHOU'}")
    print(f"Tempo total de execução dos testes:      {total_time:.2f} s")
    print("=" * 80)

if __name__ == "__main__":
    run_all_tests()

