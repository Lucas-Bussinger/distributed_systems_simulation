# MC714 - Sistemas Distribuídos: Trabalho 2
## Algoritmo 1: Eleição de Líder em Anel (Ring Election Algorithm)

Este projeto implementa o **Algoritmo de Eleição em Anel (Tanenbaum)** sobre uma topologia de **10 nós lógicos** executando em contêineres Docker independentes e comunicando-se via **gRPC**.

Os IDs de cada nó são **gerados aleatoriamente** na inicialização (IDs não-sequenciais), e o algoritmo elege como líder o nó que possuir o maior ID sorteado.

---

### 1. Modelo do Sistema e Hipóteses

- **Modelo Temporal**: Parcialmente síncrono. As chamadas RPC possuem *deadlines* e *timeouts* explícitos para detecção de indisponibilidade de nós.
- **Topologia**: Anel lógico ordenado circularmente de 10 nós:
  $$\text{no\_1} \rightarrow \text{no\_2} \rightarrow \text{no\_3} \rightarrow \dots \rightarrow \text{no\_10} \rightarrow \text{no\_1}$$
- **Identificadores Aleatórios**: Cada contêiner gera dinamicamente um ID numérico aleatório único (`random`).
- **Modelo de Falhas**: *Crash-stop*. Nós podem falhar parando suas execuções.
- **Tratamento de Falhas (Tolerância a Falhas no Anel)**:
  - Quando um nó repassa a mensagem para o seu sucessor imediato, caso ocorra timeout ou erro de rede, o nó **pula** o sucessor inativo e tenta o próximo nó na ordem circular do anel.
- **Detecção de Falha do Líder (Heartbeat)**:
  - Todos os nós que não são líderes enviam pings periódicos (`rpc Ping`) ao endereço do líder atual.
  - Após 2 falhas consecutivas de heartbeat, o líder é considerado morto e uma nova rodada de eleição é disparada automaticamente com salto sobre o nó falho.
- **Critério de Eleição**: O nó com o **maior ID numérico** entre os ativos é eleito o novo coordenador.

---

### 2. Estrutura dos Arquivos

- `protos/election.proto`: Definição dos serviços e mensagens gRPC (`RingService`, `NodeInfo`).
- `generate_protos.py`: Script para compilar o arquivo `.proto` em stubs Python (`election_pb2.py` e `election_pb2_grpc.py`).
- `node.py`: Implementação do processo do nó distribuído (IDs aleatórios dinâmicos, lógica do anel, heartbeat e tolerância a falhas).
- `client.py`: Ferramenta CLI para checar o status de todos os 10 nós ou forçar eleições manuais.
- `run_experiments.py`: Script automatizado para medição de latência, saltos e percentis (p50, p95, p99) para o relatório.
- `compose.yaml`: Configuração dos 10 serviços Docker com suas variáveis de ambiente e rede.
- `Dockerfile`: Imagem Python com suporte a gRPC e utilitários de diagnóstico de rede.

---

### 3. Como Executar os 10 Nós

Como a imagem base já está construída, basta subir os 10 contêineres:

```bash
sudo docker compose down
sudo docker compose up -d
```

Acompanhe os logs da eleição inicial em tempo real:
```bash
sudo docker compose logs -f
```

---

### 4. Demonstração e Verificação

#### A. Verificando o Status do Cluster e os IDs Aleatórios
Execute o script `client.py` de dentro de qualquer contêiner ativo:
```bash
sudo docker compose exec no_1 python client.py status
```
*Saída esperada*: Tabela mostrando todos os 10 contêineres (`no_1` a `no_10`), seus respectivos IDs gerados aleatoriamente, e o nó com o maior ID eleito como `👑 LÍDER`.

---

#### B. Injetando Falha no Líder
Identifique na tabela qual contêiner foi eleito líder (por exemplo, `no_7` ou `no_9`).
Pare o contêiner do líder atual:
```bash
sudo docker compose stop no_X
```
*(substitua `no_X` pelo contêiner do líder)*

**O que observar nos logs**:
1. Os demais 9 nós detectam a ausência do líder via falha no *heartbeat*.
2. Uma nova eleição circula pelo anel.
3. Ao tentar entregar a mensagem para o nó caído, o antecessor sofre timeout e **pula** o nó falho.
4. O nó ativo que tiver o próximo maior ID aleatório é eleito o novo líder!

Verifique o novo status:
```bash
sudo docker compose exec no_1 python client.py status
```

---

### 5. Coleta de Métricas para o Relatório (MC714)

Execute a bateria de experimentos para calcular média, desvio e percentis (p50, p95, p99) para o anel de 10 nós:
```bash
sudo docker compose exec no_1 python run_experiments.py --rounds 10
```
