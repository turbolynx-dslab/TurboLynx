#!/bin/bash
set -e

database_path=$1
queries_path=$2
query_numbers=$3
output_prefix=$4 

PROJECT_ROOT_DIR="/turbograph-v3" 
CONFIG_FILE_PATH="${PROJECT_ROOT_DIR}/src/include/common/vector_size.hpp"
BUILD_DIR="${PROJECT_ROOT_DIR}/build-release"
BUILD_COMMAND="ninja"

LOG_DIR_BASE="${PROJECT_ROOT_DIR}/logs/ablation"
current_datetime=$(date +"%Y-%m-%d")
log_dir="${LOG_DIR_BASE}/query_run_${current_datetime}"
mkdir -p ${log_dir}

# EXPERIMENT_NAMES=("base" "ablation_vectorsize" "ablation_optimizer")
# VECTOR_SIZES=(1024 64 1024)
# OPTIMIZER_MODES=("exhaustive" "exhaustive" "greedy")

EXPERIMENT_NAMES=("base" "ablation_vectorsize")
VECTOR_SIZES=(1024 64)
OPTIMIZER_MODES=("exhaustive" "exhaustive")

update_config_file() {
    local size=$1
    echo "--- Updating config file: ${CONFIG_FILE_PATH} to VECTOR_SIZE = ${size} ---"
    sed -i "s/#define STANDARD_VECTOR_SIZE [0-9]\+/#define STANDARD_VECTOR_SIZE ${size}/" ${CONFIG_FILE_PATH}
    sed -i "s/#define EXEC_ENGINE_VECTOR_SIZE [0-9]\+/#define EXEC_ENGINE_VECTOR_SIZE ${size}/" ${CONFIG_FILE_PATH}
}

parse_query_numbers() {
    IFS=';' read -ra PARTS <<< "$1"
    for part in "${PARTS[@]}"; do
        if [[ $part == *-* ]]; then
            IFS='-' read -ra RANGE <<< "$part"
            for i in $(seq "${RANGE[0]}" "${RANGE[1]}"); do
                echo "$i"
            done
        else
            echo "$part"
        fi
    done
}

queries=$(parse_query_numbers $query_numbers)
CURRENTLY_COMPILED_SIZE=-1

trap "echo; echo '--- CLEANUP: Reverting config file to original state...'; cd ${PROJECT_ROOT_DIR}; git checkout -- ${CONFIG_FILE_PATH}; echo 'Cleanup complete.'" EXIT SIGHUP SIGINT SIGTERM

for i in "${!EXPERIMENT_NAMES[@]}"; do
    EXP_NAME=${EXPERIMENT_NAMES[$i]}
    TARGET_VSIZE=${VECTOR_SIZES[$i]}
    TARGET_OPTIMIZER=${OPTIMIZER_MODES[$i]}

    echo ""
    echo "================================================="
    echo "  STARTING EXPERIMENT: $EXP_NAME (VSize: $TARGET_VSIZE, Optimizer: $TARGET_OPTIMIZER)"
    echo "================================================="

    if [ "$TARGET_VSIZE" != "$CURRENTLY_COMPILED_SIZE" ]; then
        echo "Recompilation required (Target: $TARGET_VSIZE, Current: $CURRENTLY_COMPILED_SIZE)"
        update_config_file $TARGET_VSIZE
        
        echo "Recompiling project..."
        cd ${BUILD_DIR}
        (time ${BUILD_COMMAND}) 2>&1 | tee "${log_dir}/compile_log_${EXP_NAME}.txt"
        echo "Compile complete."
        
        CURRENTLY_COMPILED_SIZE=$TARGET_VSIZE
    else
        echo "No recompilation required (Target: $TARGET_VSIZE, Current: $CURRENTLY_COMPILED_SIZE)"
    fi

    output_file="${output_prefix}_${EXP_NAME}.csv"
    echo "Running queries... Outputting CSV to ${output_file}"
    echo "QueryNumber,CompileTime,QueryExecutionTime,EndtoEndTime" > $output_file

    for query_num in $queries; do
        query_file="${queries_path}/q${query_num}.cql"
        if [ ! -f "$query_file" ]; then
            echo "Query file $query_file not found! Skipping."
            continue
        fi
        
        log_file_query="${log_dir}/q${query_num}_${EXP_NAME}.txt"
        query_str=$(cat "$query_file")

        echo "Running Q${query_num} ($EXP_NAME)... "
        
        output_str=$(timeout 3600s \
            ${BUILD_DIR}/tools/client \
            --standalone \
            --slient \
            --workspace ${database_path} \
            --query "${query_str}" \
            --disable-merge-join \
            --iterations 3 \
            --join-order-optimizer ${TARGET_OPTIMIZER} \
            --warmup)

        echo "$output_str" >> "$log_file_query"

        compile_time=$(echo "$output_str" | grep -oP 'Average Compile Time: \K[0-9.]+' | awk '{printf "%.2f", $1}')
        exec_time=$(echo "$output_str" | grep -oP 'Average Query Execution Time: \K[0-9.]+' | awk '{printf "%.2f", $1}')
        end_to_end_time=$(echo "$output_str" | grep -oP 'Average End to End Time: \K[0-9.]+' | awk '{printf "%.2f", $1}')

        echo "Q$query_num,$compile_time,$exec_time,$end_to_end_time" >> "$output_file"
    done
    
    echo "Experiment for $EXP_NAME complete. CSV saved to ${output_file}"
done

echo "--- All experiments finished. ---"