#!/bin/bash

gumby/experiments/trustchain/post_process_trustchain.py .

# Run the regular process guard script
graph_process_guard_data.sh
