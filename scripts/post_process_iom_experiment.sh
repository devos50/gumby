#!/bin/bash

# Parse statistics about the market community
gumby/experiments/internetofmoney/parse_iom_statistics.py .

# Run the regular Dispersy message extraction script
post_process_dispersy_experiment.sh
