#!/usr/bin/env bash
gumby/experiments/trustchain/post_process_trustchain.py .

# Parse IPv8 statistics
gumby/experiments/ipv8/parse_ipv8_statistics.py .
graph_ipv8_stats.sh

# Run the regular statistics extraction script
graph_process_guard_data.sh
