#!/bin/bash
set -e

SCRIPT_DIR=$(dirname "$(realpath $0)")
STG=${SCRIPT_DIR}
OUTPUT_DIR=${SCRIPT_DIR}/results

# GPT-3 175B
dmodel=12288
dff=49152
batch=8
# micro_batch=8
seq=1024
head=128
# kvhead=128
num_stacks=1
experts=1
# kexperts=8

model_type="llm_forward"
templates="decoding"

dp=1
tp=8
pp=1
sp=1
ep=1

# Run Symbolic Tensor Graph (STG) Generator
(
 python -m cProfile -o profile_output.prof ${STG}/main_adv.py \
               --output_dir ${OUTPUT_DIR} \
               --output_name workload.%d.et \
               --dp ${dp} --tp ${tp} --pp ${pp} --sp ${sp} --ep ${ep}\
               --dmodel ${dmodel} \
               --dff ${dff} \
               --experts ${experts} \
               --kexperts ${experts} \
               --head ${head} \
               --kvhead ${head} \
               --batch ${batch} \
               --micro_batch ${batch} \
               --seq ${seq} \
               --num_stacks ${num_stacks} \
               --templates ${templates} --model_type ${model_type}
)