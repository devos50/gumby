#!/bin/bash

# Parse statistics about the Waves experiment
gumby/experiments/waves/parse_waves_statistics.py .

# Run the regular process guard script
graph_process_guard_data.sh
