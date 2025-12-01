import matplotlib.pyplot as plt
import numpy as np
import os

benchmarks = ['DBpedia', 'DBpedia-Ext']

# 전체 설정 (Baseline 포함)
all_configurations = [
    'Baseline',
    '+ CGC',
    '+ CGC, SSRF',
    '+ CGC, SSRF, GEM'
]

# Raw Data (ms) - [DBpedia, DBpedia-Ext]
# DBpedia-Ext는 데이터가 좀 더 크고, 기법 적용시 성능 향상이 점진적으로 보이도록 설정
raw_data = [
    [1544.05, 3500.00],  # Baseline (기준)
    [526.82,  1256.00],  # + CGC (약 2.5배)
    [526.29,  548.00],  # + SSRF (약 3.1배 - 추가 향상)
    [531.09,  499.00]    # + GEM (약 3.8배 - 최종 향상)
]

# --- Data Processing ---
baseline_times = raw_data[0] # [1544.05, 3500.00]
full_speedups = []

# 전체 Speedup 계산 (Baseline / Target)
for config_times in raw_data:
    # 각 벤치마크(j)에 대해 계산
    speedups = [baseline_times[j] / config_times[j] for j in range(len(benchmarks))]
    full_speedups.append(speedups)

# *** Baseline(인덱스 0) 제외 ***
configurations = all_configurations[1:] 
plot_data = full_speedups[1:] # [[DB_s1, Ext_s1], [DB_s2, Ext_s2], ...] 로 구성됨

# 스타일 설정 (Baseline 제외)
colors = ['lightgray', 'silver', 'gray']
hatches = ['///', '\\\\\\', '...']

# --- Plotting ---
n_benchmarks = len(benchmarks)
n_configs = len(configurations) # 3개 (+CGC, +SSRF, +GEM)

bar_width = 0.2 
index = np.arange(n_benchmarks)

fig, ax = plt.subplots(figsize=(6, 3.5)) # 너비를 조금 늘림

for i in range(n_configs):
    # 각 기법(i)에 대한 벤치마크별 데이터 추출
    current_config_data = plot_data[i] # [DBpedia_speedup, Ext_speedup]
    
    # 막대 위치 조정
    position = index + (i - (n_configs - 1) / 2) * bar_width
    
    ax.bar(
        position, 
        current_config_data, 
        bar_width, 
        label=configurations[i],
        color=colors[i],
        hatch=hatches[i],
        edgecolor='black'
    )

# *** 은은한 Baseline 기준선 (y=1.0) ***
ax.axhline(y=1.0, color='black', linestyle='--', linewidth=1.0, alpha=0.4)
# (선택사항) 기준선 옆에 작게 글씨 추가하고 싶다면 아래 주석 해제
# ax.text(1.3, 1.05, 'Baseline', fontsize=10, color='gray', va='bottom')

# Y축 라벨 (두 줄 처리)
ax.set_ylabel('Speedup over \n Baseline (x)', fontsize=18)
# ax.set_xlabel('Benchmark', fontsize=15) # 벤치마크 이름이 명확하면 생략 가능

ax.set_xlabel('Benchmark', fontsize=18)
ax.set_xticks(index)
ax.set_xticklabels(benchmarks, fontsize=17) 
ax.tick_params(axis='y', labelsize=13)

# 틱 설정 (최대 4배 정도까지 나오므로 0~4.5 설정)
ticks = [0, 1, 2, 3, 4, 5, 6, 7]
ax.set_yticks(ticks)
ax.set_ylim(0, 8) 

ax.grid(axis='y', linestyle='--', alpha=0.5)

# 범례 설정
ax.legend(
    loc='upper center',         # 위쪽 중앙 배치
    bbox_to_anchor=(0.5, 1.15), # 그래프 바깥 상단
    ncol=3,                     # 가로로 길게 배치
    fontsize=11,
    frameon=False,              # 테두리 제거 (깔끔하게)
    columnspacing=1.5
)

plt.tight_layout()
output_path = os.path.join('./', 'cgc-ssrf-gem-ablation.pdf')
plt.savefig(output_path, dpi=800, bbox_inches='tight')
plt.show()