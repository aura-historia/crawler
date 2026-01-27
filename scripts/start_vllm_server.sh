#!/bin/bash

# vLLM Server Start Script
# This script starts the vLLM OpenAI-compatible server for the scraper worker

set -e

# Configuration
MODEL_NAME="unsloth/Qwen2.5-7B-Instruct-bnb-4bit"
HOST="${VLLM_HOST:-localhost}"
PORT="${VLLM_PORT:-8003}"
MAX_MODEL_LEN="${VLLM_MAX_MODEL_LEN:-8000}"
GPU_MEMORY_UTIL="${VLLM_GPU_MEMORY_UTIL:-0.85}"
MAX_NUM_SEQS="${VLLM_MAX_NUM_SEQS:-8}"
DTYPE="${VLLM_DTYPE:-auto}"

echo "════════════════════════════════════════════════════════════════"
echo "  Starting vLLM OpenAI-Compatible Server"
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "Configuration:"
echo "  Model:                 $MODEL_NAME"
echo "  Host:                  $HOST"
echo "  Port:                  $PORT"
echo "  Max Model Length:      $MAX_MODEL_LEN"
echo "  GPU Memory Util:       $GPU_MEMORY_UTIL"
echo "  Max Num Sequences:     $MAX_NUM_SEQS"
echo "  Data Type:             $DTYPE"
echo ""
echo "The server will be accessible at: http://$HOST:$PORT"
echo ""
echo "Press Ctrl+C to stop the server"
echo "════════════════════════════════════════════════════════════════"
echo ""

# Check if vllm is installed
if ! command -v vllm &> /dev/null; then
    echo "Error: vllm is not installed"
    echo "Install it with: pip install vllm"
    exit 1
fi

# Start the server
exec vllm serve "$MODEL_NAME" \
    --host "$HOST" \
    --port "$PORT" \
    --max-model-len "$MAX_MODEL_LEN" \
    --gpu-memory-utilization "$GPU_MEMORY_UTIL" \
    --max-num-seqs "$MAX_NUM_SEQS" \
    --dtype "$DTYPE" \
    --trust-remote-code\
    --enable-prefix-caching\
    --enable-chunked-prefill\
    --structured-outputs-config.backend xgrammar \


