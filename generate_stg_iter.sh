#!/bin/bash
# set -e

# Path
SCRIPT_DIR=$(dirname "$(realpath $0)")
STG=${SCRIPT_DIR}

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --dmodel) dmodel="$2"; shift 2 ;;
        --dff) dff="$2"; shift 2 ;;
        --batch) batch="$2"; shift 2 ;;
        --seq) seq="$2"; shift 2 ;;
        --num_stacks) num_stacks="$2"; shift 2 ;;
        --head) head="$2"; shift 2 ;;
        --dp) dp="$2"; shift 2 ;;
        --tp) tp="$2"; shift 2 ;;
        --pp) pp="$2"; shift 2 ;;
        --output_dir) output_dir="$2"; shift 2 ;;
        --model_type) model_type="$2"; shift 2 ;;
        --templates) templates="$2"; shift 2 ;;
        *) echo "Unknown parameter: $1"; exit 1 ;;
    esac
done

# Set defaults if not provided
model_type=${model_type:-llm_forward}
templates=${templates:-decoding}
output_dir=${output_dir:-./iteration_generated/}

# Create output directory
mkdir -p "${output_dir}"

echo "STG Generation Parameters:"
echo "  Model: dmodel=${dmodel}, dff=${dff}, head=${head}"
echo "  Batch: batch=${batch}, seq=${seq}, num_stacks=${num_stacks}"
echo "  Parallelism: dp=${dp}, tp=${tp}, pp=${pp}"
echo "  Model type: ${model_type}, templates: ${templates}"
echo "  Output: ${output_dir}"

# Run Symbolic Tensor Graph (STG) Generator using main_adv.py
python ${STG}/main_adv.py \
    --output_dir "${output_dir}" \
    --output_name workload.%d.et \
    --dp ${dp} \
    --tp ${tp} \
    --pp ${pp} \
    --dmodel ${dmodel} \
    --dff ${dff} \
    --experts 1 --kexperts 1 \
    --head ${head} --kvhead ${head} \
    --batch ${batch} --micro_batch ${batch} \
    --seq ${seq} \
    --num_stacks ${num_stacks} \
    --templates ${templates} \
    --model_type ${model_type}

echo "STG generation completed. Output saved to: ${output_dir}"