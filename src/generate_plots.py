import matplotlib.pyplot as plt
import numpy as np
import os

# Dados experimentais coletados (N = 3, 5, 10)
nodes = np.array([3, 5, 10])

# Eleição em Anel (Chang & Roberts)
election_mean = np.array([10.19, 15.76, 25.81])
election_std = np.array([1.40, 2.40, 1.73])
election_p50 = np.array([9.62, 15.11, 25.66])
election_p95 = np.array([13.29, 20.08, 28.82])
election_p99 = np.array([13.29, 21.60, 29.45])

# Two-Phase Commit (2PC)
tpc_mean = np.array([7.75, 11.00, 21.06])
tpc_std = np.array([1.08, 1.03, 1.55])
tpc_p50 = np.array([7.41, 10.95, 21.22])
tpc_p95 = np.array([9.40, 12.43, 23.44])
tpc_p99 = np.array([9.67, 13.33, 24.70])

# Configurações globais de estilo
plt.rcParams.update({
    'font.size': 11,
    'axes.labelsize': 12,
    'axes.titlesize': 13,
    'xtick.labelsize': 11,
    'ytick.labelsize': 11,
    'legend.fontsize': 10,
    'figure.titlesize': 14,
    'lines.linewidth': 2,
    'lines.markersize': 7
})

output_dir = 'report'
os.makedirs(output_dir, exist_ok=True)

# -------------------------------------------------------------
# Gráfico 1: Eleição em Anel (Latência de Convergência)
# -------------------------------------------------------------
fig, ax = plt.subplots(figsize=(6.5, 4.2), dpi=300)

ax.errorbar(nodes, election_mean, yerr=election_std, fmt='-o', color='#1f77b4',
            capsize=5, capthick=1.5, elinewidth=1.5, label='Média ± 1 Desvio Padrão', zorder=4)
ax.plot(nodes, election_p50, '--s', color='#2ca02c', label='Mediana (p50)', zorder=3)
ax.plot(nodes, election_p95, '-.^', color='#ff7f0e', label='Percentil 95 (p95)', zorder=3)
ax.plot(nodes, election_p99, ':d', color='#d62728', label='Percentil 99 (p99)', zorder=3)

# Destaques nos pontos
for n, m, p95 in zip(nodes, election_mean, election_p95):
    ax.annotate(f"{m:.1f} ms", (n, m), textcoords="offset points", xytext=(0, 8), ha='center', fontsize=9, fontweight='bold', color='#1f77b4')

ax.set_title('Convergência da Eleição em Anel (Chang & Roberts)')
ax.set_xlabel('Número de Nós Ativos (N)')
ax.set_ylabel('Tempo de Convergência (ms)')
ax.set_xticks(nodes)
ax.set_ylim(5, 35)
ax.grid(True, linestyle='--', alpha=0.6)
ax.legend(loc='upper left', frameon=True)
plt.tight_layout()

fig.savefig(os.path.join(output_dir, 'grafico_eleicao.png'), dpi=300)
fig.savefig(os.path.join(output_dir, 'grafico_eleicao.pdf'))
plt.close(fig)

# -------------------------------------------------------------
# Gráfico 2: Two-Phase Commit (Latência Fim-a-Fim)
# -------------------------------------------------------------
fig, ax = plt.subplots(figsize=(6.5, 4.2), dpi=300)

ax.errorbar(nodes, tpc_mean, yerr=tpc_std, fmt='-o', color='#9467bd',
            capsize=5, capthick=1.5, elinewidth=1.5, label='Média ± 1 Desvio Padrão', zorder=4)
ax.plot(nodes, tpc_p50, '--s', color='#2ca02c', label='Mediana (p50)', zorder=3)
ax.plot(nodes, tpc_p95, '-.^', color='#ff7f0e', label='Percentil 95 (p95)', zorder=3)
ax.plot(nodes, tpc_p99, ':d', color='#d62728', label='Percentil 99 (p99)', zorder=3)

# Destaques nos pontos
for n, m in zip(nodes, tpc_mean):
    ax.annotate(f"{m:.1f} ms", (n, m), textcoords="offset points", xytext=(0, 8), ha='center', fontsize=9, fontweight='bold', color='#9467bd')

ax.set_title('Latência de Commit Atômico (Two-Phase Commit)')
ax.set_xlabel('Número de Participantes (N)')
ax.set_ylabel('Tempo Total de Commit (ms)')
ax.set_xticks(nodes)
ax.set_ylim(4, 30)
ax.grid(True, linestyle='--', alpha=0.6)
ax.legend(loc='upper left', frameon=True)
plt.tight_layout()

fig.savefig(os.path.join(output_dir, 'grafico_2pc.png'), dpi=300)
fig.savefig(os.path.join(output_dir, 'grafico_2pc.pdf'))
plt.close(fig)

# -------------------------------------------------------------
# Gráfico 3: Comparativo Lado a Lado / Barras Agrupadas
# -------------------------------------------------------------
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.2), dpi=300)

# Subplot 1: Médias Comparadas com Intervalo de Incerteza
ax1.errorbar(nodes, election_mean, yerr=election_std, fmt='-o', color='#1f77b4',
             capsize=4, label='Eleição (Anel)', linewidth=2)
ax1.errorbar(nodes, tpc_mean, yerr=tpc_std, fmt='-s', color='#9467bd',
             capsize=4, label='2PC (Coordenador-Estrela)', linewidth=2)
ax1.set_title('(a) Escalonamento da Latência Média')
ax1.set_xlabel('Tamanho do Cluster (N)')
ax1.set_ylabel('Latência Média (ms)')
ax1.set_xticks(nodes)
ax1.grid(True, linestyle='--', alpha=0.6)
ax1.legend(loc='upper left')

# Subplot 2: Cauda de Distribuição (p50 vs p95 vs p99)
width = 0.22
x = np.arange(len(nodes))

rects1 = ax2.bar(x - width, election_mean, width, label='Eleição (Média)', color='#3274a1')
rects2 = ax2.bar(x, tpc_mean, width, label='2PC (Média)', color='#8b5fa8')
rects3 = ax2.bar(x + width, election_p95, width, label='Eleição (p95)', color='#6baed6')
rects4 = ax2.bar(x + 2*width, tpc_p95, width, label='2PC (p95)', color='#bcbddc')

ax2.set_title('(b) Comparação de Cauda (Média vs p95)')
ax2.set_xlabel('Tamanho do Cluster (N)')
ax2.set_ylabel('Tempo (ms)')
ax2.set_xticks(x + width/2)
ax2.set_xticklabels([f'{n} nós' for n in nodes])
ax2.grid(True, linestyle='--', alpha=0.6, axis='y')
ax2.legend(loc='upper left', fontsize=9)

plt.tight_layout()
fig.savefig(os.path.join(output_dir, 'grafico_comparativo.png'), dpi=300)
fig.savefig(os.path.join(output_dir, 'grafico_comparativo.pdf'))
plt.close(fig)

print("Todos os gráficos foram gerados com sucesso na pasta report/ em PNG e PDF!")
