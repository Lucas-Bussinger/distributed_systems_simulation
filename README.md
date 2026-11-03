# MC714 - Sistemas Distribuídos: Trabalho 2
## Implementação e Avaliação Experimental de Algoritmos Distribuídos

Este projeto contém a implementação de dois algoritmos distribuídos clássicos executando em contêineres Docker independentes (`python:3.10-slim`) e comunicando-se exclusivamente via **gRPC sobre TCP**:

1. **Algoritmo de Eleição de Líder em Anel** (*Ring Election Algorithm* - van Steen & Tanenbaum): Topologia circular com IDs aleatórios não-sequenciais, salto sobre nós inativos (*skip*) e detecção de falhas via *heartbeat*.
2. **Protocolo 2PC (Two-Phase Commit) Bancário**: Coordenação de transações financeiras distribuídas (atualização atômica de saldos e controle de concorrência com locks em memória), utilizando o Líder eleito como Coordenador e os demais nós como réplicas/coortes.

---

## 1. Estrutura do Diretório

```
distributed_systems_simulation/
├── protos/                          # Contratos de comunicação gRPC (.proto)
│   ├── election.proto               # Serviços do Anel de Eleição e Heartbeat
│   └── bank_2pc.proto               # Métodos das Fases 1 (Prepare) e 2 (Commit/Abort) do 2PC
│
├── src/                             # Código-fonte Python
│   ├── node.py                      # Nó lógico (Anel + Coordenador/Coorte 2PC)
│   ├── client.py                    # CLI de inspeção e controle da Eleição em Anel
│   ├── client_2pc.py                # CLI para consulta de saldos, locks e transferências
│   ├── test_2pc_scenarios.py        # Suíte automatizada de testes e cenários de falha do 2PC
│   ├── run_experiments.py           # Coleta de métricas e percentis do Algoritmo do Anel
│   ├── run_2pc_experiments.py       # Coleta de métricas e percentis do 2PC (3, 5 e 10 nós)
│   └── generate_protos.py           # Compilação dos arquivos .proto em stubs Python
│
├── docker/                          # Arquivos de contêiner e orquestração
│   ├── Dockerfile                   # Imagem base dos nós com dependências gRPC
│   ├── compose.yaml                 # Configuração padrão com 10 nós lógicos
│   ├── compose-3nodes.yaml          # Configuração com 3 nós lógicos
│   └── compose-5nodes.yaml          # Configuração com 5 nós lógicos
│
├── compose.yaml                     # Redirecionador para docker/compose.yaml
└── requirements.txt                 # Dependências Python (grpcio, grpcio-tools)
```

---

## 2. Inicialização do Ambiente

### Construir as imagens e iniciar o cluster (10 nós):
```bash
sudo docker compose down
sudo docker compose up -d --build
```

### Visualizar os logs da eleição inicial em tempo real:
```bash
sudo docker compose logs -f
```

---

## 3. Comandos de Verificação: Algoritmo de Eleição em Anel

### A. Consultar status de todos os nós e identificador do líder eleito:
```bash
sudo docker compose exec no_1 python src/client.py status
```

### B. Disparar uma eleição manual a partir de qualquer nó:
```bash
sudo docker compose exec no_1 python src/client.py trigger --node no_2:50051
```

### C. Simular queda de um nó intermediário (salto no anel):
```bash
# 1. Parar um nó intermediário (ex: no_3)
sudo docker compose stop no_3

# 2. Disparar eleição a partir do nó anterior
sudo docker compose exec no_1 python src/client.py trigger --node no_2:50051

# 3. Observar nos logs o nó anterior pulando o nó inativo
sudo docker compose logs no_2 | tail -n 20

# 4. Verificar a integridade do cluster e o novo líder
sudo docker compose exec no_1 python src/client.py status

# 5. Reiniciar o nó
sudo docker compose start no_3
```

### D. Simular queda do Líder atual (detecção via heartbeat e re-eleição):
```bash
# 1. Identificar qual contêiner é o líder atual (coluna PAPEL)
sudo docker compose exec no_1 python src/client.py status

# 2. Derrubar o nó que atua como líder (substitua no_X pelo contêiner líder identificado)
sudo docker compose stop no_X

# 3. Acompanhar os nós detectando a ausência de resposta e elegendo o novo líder
sudo docker compose logs -f no_1

# 4. Verificar o novo líder eleito na tabela
sudo docker compose exec no_1 python src/client.py status

# 5. Reiniciar o nó anterior
sudo docker compose start no_X
```

---

## 4. Comandos de Verificação: Protocolo 2PC Bancário

### A. Consultar saldos contábeis e locks em todas as réplicas:
```bash
sudo docker compose exec no_1 python src/client_2pc.py balances
```

### B. Executar transferência válida (Fase 1 Voto SIM -> Fase 2 Commit):
```bash
sudo docker compose exec no_1 python src/client_2pc.py transfer --source conta_A --dest conta_B --amount 100
```
Verificar que todas as réplicas atualizaram os saldos de forma idêntica:
```bash
sudo docker compose exec no_1 python src/client_2pc.py balances
```

### C. Simular falha por regra de negócio (Saldo Insuficiente -> Abort e Rollback):
```bash
sudo docker compose exec no_1 python src/client_2pc.py transfer --source conta_A --dest conta_B --amount 99999
```
Confirmar que os saldos foram integralmente preservados em todos os nós:
```bash
sudo docker compose exec no_1 python src/client_2pc.py balances
```

### D. Simular falha por conta inexistente:
```bash
sudo docker compose exec no_1 python src/client_2pc.py transfer --source conta_Fantasma --dest conta_B --amount 50
```

### E. Simular queda de participante na Fase 1 (Timeout gRPC -> Abort):
```bash
# 1. Parar um dos participantes seguidores (ex: no_4)
sudo docker compose stop no_4

# 2. Tentar efetuar uma transferência via líder
sudo docker compose exec no_1 python src/client_2pc.py transfer --source conta_A --dest conta_B --amount 50

# 3. Constatar que a ausência do nó foi computada como NÃO e a transação abortou
sudo docker compose exec no_1 python src/client_2pc.py balances

# 4. Reiniciar o participante e restabelecer sincronização
sudo docker compose start no_4
sudo docker compose exec no_1 python src/client_2pc.py sync --node no_4:50051
```

### F. Simular queda crítica do Líder pós-Fase 1 (Problema do Bloqueio):
```bash
# 1. Submeter transação que injeta crash no líder logo após coletar votos SIM da Fase 1
sudo docker compose exec no_1 python src/client_2pc.py transfer --source conta_A --dest conta_B --amount 75 --crash-leader

# 2. Constatar imediatamente que os nós retiveram o lock sobre as contas (estado PREPARED bloqueante)
sudo docker compose exec no_1 python src/client_2pc.py balances

# 3. Observar no terminal a eleição de um Novo Líder pelo Algoritmo do Anel
sudo docker compose logs -f no_1

# 4. Sincronizar o cluster e desobstruir a transação bloqueada via Novo Líder
sudo docker compose exec no_1 python src/client_2pc.py sync

# 5. Confirmar liberação dos locks e integridade dos saldos
sudo docker compose exec no_1 python src/client_2pc.py balances
```

---

## 5. Execução Automatizada da Suíte de Testes

Para executar toda a rotina de validação e testes automatizados dos cenários de execução normal e cenários de falha em um único comando:

```bash
sudo docker compose exec no_1 python src/test_2pc_scenarios.py
```

---

## 6. Coleta de Métricas Experimentais

### 6.1 Algoritmo 1: Eleição em Anel (Tempo de Convergência e Percentis)

#### Experimento em 10 Nós (Topologia Padrão):
```bash
sudo docker compose exec no_1 python src/run_experiments.py --rounds 20
```

#### Experimento em 5 Nós:
```bash
sudo docker compose -f docker/compose-5nodes.yaml down
sudo docker compose -f docker/compose-5nodes.yaml up -d
sudo docker compose -f docker/compose-5nodes.yaml exec no_1 python src/run_experiments.py --rounds 20
```

#### Experimento em 3 Nós:
```bash
sudo docker compose -f docker/compose-3nodes.yaml down
sudo docker compose -f docker/compose-3nodes.yaml up -d
sudo docker compose -f docker/compose-3nodes.yaml exec no_1 python src/run_experiments.py --rounds 20
```

---

### 6.2 Algoritmo 2: Protocolo 2PC (Latência de Transação e Percentis)

#### Experimento em 10 Nós (Topologia Padrão):
```bash
sudo docker compose down
sudo docker compose up -d
sudo docker compose exec no_1 python src/run_2pc_experiments.py --rounds 20
```

#### Experimento em 5 Nós:
```bash
sudo docker compose -f docker/compose-5nodes.yaml down
sudo docker compose -f docker/compose-5nodes.yaml up -d
sudo docker compose -f docker/compose-5nodes.yaml exec no_1 python src/run_2pc_experiments.py --rounds 20
```

#### Experimento em 3 Nós:
```bash
sudo docker compose -f docker/compose-3nodes.yaml down
sudo docker compose -f docker/compose-3nodes.yaml up -d
sudo docker compose -f docker/compose-3nodes.yaml exec no_1 python src/run_2pc_experiments.py --rounds 20
```

---

## 7. o que é demonstrado no vídeo do projeto:

```bash
# PASSO 1: Subir o cluster e demonstrar o status inicial com líder eleito
sudo docker compose up -d
sudo docker compose exec no_1 python src/client.py status
sudo docker compose exec no_1 python src/client_2pc.py balances

# PASSO 2: Demonstrar uma transação bancária 2PC normal (Commit)
sudo docker compose exec no_1 python src/client_2pc.py transfer --source conta_A --dest conta_B --amount 100
sudo docker compose exec no_1 python src/client_2pc.py balances

# PASSO 3: Demonstrar rejeição por regra de negócio (Saldo Insuficiente)
sudo docker compose exec no_1 python src/client_2pc.py transfer --source conta_A --dest conta_B --amount 99999
sudo docker compose exec no_1 python src/client_2pc.py balances

# PASSO 4: Injeção de Falha em Participante (docker stop) e recuperação
sudo docker compose stop no_3
sudo docker compose exec no_1 python src/client_2pc.py transfer --source conta_A --dest conta_B --amount 50
sudo docker compose start no_3
sudo docker compose exec no_1 python src/client_2pc.py sync --node no_3:50051

# PASSO 5: Injeção de Falha Crítica (Queda do Líder) e eleição do novo Líder
sudo docker compose exec no_1 python src/client.py status
sudo docker compose stop <container_lider>
sudo docker compose logs -f no_1
sudo docker compose exec no_1 python src/client.py status
sudo docker compose start <container_lider>

# PASSO 6: Execução da suíte de testes automatizada final
sudo docker compose exec no_1 python src/test_2pc_scenarios.py
```

---

## 8. Limpeza do Ambiente

Para encerrar e remover todos os contêineres e redes criadas:
```bash
sudo docker compose down -v
```
