import matplotlib.pyplot as plt
import numpy as np
import os
from matplotlib.ticker import ScalarFormatter

benchmarks = ['LDBC', 'TPC-H', 'DBpedia']

configurations = [
    'w/o Vectorization',
    'w/o Exhaustive Opt.',
    'w/o Adj. List Index'
]

data = {
    'w/o Vectorization': [3.54, 37.80, 52.80],
    'w/o Exhaustive Opt.': [1.39, 2.30, 0.99],
    'w/o Adj. List Index': [31.8, 4.56, 45.13]
}

plot_data = [data[config] for config in configurations]

n_benchmarks = len(benchmarks)  # 벤치마크 개수 (3)
n_configs = len(configurations)   # 설정 개수 (4)

bar_width = 0.2

index = np.arange(n_benchmarks)

fig, ax = plt.subplots(figsize=(5, 3))

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
        edgecolor='black'  # 예시 그래프처럼 검은색 테두리 추가
    )


# 4. 축 레이블 및 제목 설정
# -------------------------------------------------
ax.set_ylabel('Query Time \n Relative Slowdown', fontsize=15)
ax.set_xlabel('Benchmark', fontsize=15)

# 5. X축 틱 및 레이블 설정
# -------------------------------------------------
# X축 틱을 각 그룹의 중앙(0, 1, 2)으로 설정
ax.set_xticks(index)
ax.set_xticklabels(benchmarks, fontsize=13)
ax.tick_params(axis='y', labelsize=15)
ax.set_yscale('log')
ticks = [1, 10, 50]
ax.set_yticks(ticks)
ax.yaxis.set_major_formatter(ScalarFormatter())

max_value = max(max(v) for v in plot_data)
ax.set_ylim(0, max_value * 8)
ax.grid(axis='y', linestyle='--', alpha=0.7)

ax.legend(loc='upper center', fontsize=12, bbox_to_anchor=(0.43, 1.03))
plt.tight_layout()
output_path = os.path.join('./', 'ablation_component_graph.pdf')
plt.savefig(output_path, dpi=800, bbox_inches='tight')