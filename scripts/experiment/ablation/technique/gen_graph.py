import matplotlib.pyplot as plt
import numpy as np
import os

benchmarks = ['DBpedia']

configurations = [
    'Baseline',
    '+ CGC',
    '+ CGC, SSRF',
    '+ CGC, SSRF, GEM'
]

plot_data = [
    [2770.05],
    [2296.24],
    [1739.54],
    [1522.3]
]

n_benchmarks = len(benchmarks)  # 1
n_configs = len(configurations)   # 4

bar_width = 0.2 
index = np.arange(n_benchmarks) # [0]

fig, ax = plt.subplots(figsize=(5, 3)) # 참조 코드와 동일한 figsize

colors = ['white', 'lightgray', 'silver', 'gray']
hatches = [' ', '///', '\\\\\\', '...']

for i in range(n_configs):
    position = index + (i - (n_configs - 1) / 2) * bar_width
    
    ax.bar(
        position, 
        plot_data[i], 
        bar_width, 
        label=configurations[i],
        color=colors[i],
        hatch=hatches[i],
        edgecolor='black'
    )

ax.set_ylabel('Avgerage Query \n Execution Time (ms)', fontsize=15) 
ax.set_xlabel('Benchmark', fontsize=15)

ax.set_xticks(index)
ax.set_xticklabels(benchmarks, fontsize=13) 
ax.tick_params(axis='y', labelsize=15)

ticks = [0, 1000, 2000, 3000, 4000]
ax.set_yticks(ticks)

ax.set_ylim(0, 4300) # 틱에 맞춰 3000으로 설정
ax.grid(axis='y', linestyle='--', alpha=0.7) # 그리드 유지

ax.legend(
    loc='upper right', 
    bbox_to_anchor=(1.02, 1.03), # 그래프 상단 중앙
    fontsize=12,               # 참조 코드의 폰트 크기
    labelspacing=0.2
)

plt.tight_layout()
output_path = os.path.join('./', 'ablation_tehcnique_graph.pdf')
plt.savefig(output_path, dpi=800, bbox_inches='tight')