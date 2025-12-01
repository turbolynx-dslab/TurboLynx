#!/bin/bash

# Input parameters
database_path_base=/data/ldbc-chunk-size/
queries_path=/turbograph-v3/queries/ldbc/sf100/
query_numbers=12

log_dir_base="/turbograph-v3/logs"
current_datetime=$(date +"%Y-%m-%d")
log_dir="${log_dir_base}/query/${current_datetime}"
mkdir -p ${log_dir}

# Function to parse query numbers
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


#vector_size=("130816" "261888" "524032" "1048320")
vector_size=("130816" "1048320")

vector_size_file_path="/turbograph-v3/src/include/common/vector_size.hpp"

update_vector_size_file() {
    local new_size=$1
    local file_path=$vector_size_file_path

    if [ ! -f "$file_path" ]; then
        echo "Error: File not found at $file_path"
        exit 1
    fi

    echo "[Config Update] Setting STORAGE_STANDARD_VECTOR_SIZE to $new_size in $file_path"
    
    sed -i "s/^#define STORAGE_STANDARD_VECTOR_SIZE .*/#define STORAGE_STANDARD_VECTOR_SIZE ${new_size}/" "$file_path"
}

# Parse query numbers
queries=$(parse_query_numbers $query_numbers)

for vs in "${vector_size[@]}"; do
    update_vector_size_file ${vs}
    cd /turbograph-v3/build-release && ninja
    cd -

    if [ "$vs" -eq "1048320" ]; then
        database_path="/data/ldbc/sf100" 
    else
        database_path="${database_path_base}/sf100-vs${vs}"
    fi
    output_file="ldbc_chunk_size_vs${vs}.csv"

    echo "QueryNumber,CompileTime,QueryExecutionTime,EndtoEndTime" > $output_file

    # Execute queries
    for query_num in $queries; do
        query_file="${queries_path}/q${query_num}.cql"
        if [ ! -f "$query_file" ]; then
            echo "Query file $query_file not found!"
            continue
        fi
        log_file="${log_dir}/q${query_num}.txt"
        query_str=$(cat "$query_file")

        # Run query
        #output_str=$(timeout 3600s \
        #    /turbograph-v3/build-release/tools/client \
        #    --standalone \
        #    --slient \
        #    --workspace ${database_path} \
        #    --query "${query_str}" \
        #    --disable-merge-join \
        #    --iterations 1 \
        #    --join-order-optimizer greedy)
        /turbograph-v3/build-release/tools/client \
            --standalone \
            --slient \
            --workspace ${database_path} \
            --query "${query_str}" \
            --disable-merge-join \
            --iterations 1 \
            --join-order-optimizer greedy

        # Output to log file
        echo "$output_str" >> "$log_file"

        # Extract times
        compile_time=$(echo "$output_str" | grep -oP 'Average Compile Time: \K[0-9.]+' | awk '{printf "%.2f", $1}')
        exec_time=$(echo "$output_str" | grep -oP 'Average Query Execution Time: \K[0-9.]+' | awk '{printf "%.2f", $1}')
        end_to_end_time=$(echo "$output_str" | grep -oP 'Average End to End Time: \K[0-9.]+' | awk '{printf "%.2f", $1}')

        echo "Q$query_num, $compile_time, $exec_time, $end_to_end_time" >> "$output_file"
    done
done
