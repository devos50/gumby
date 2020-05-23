#!/bin/bash

while read -r SERVER
do
  cmd="AWS_INSTANCES_TO_RUN=$AWS_INSTANCES_TO_RUN GUMBY_LOG_LEVEL=$GUMBY_LOG_LEVEL AWS_NODE_AMOUNT=$AWS_NODE_AMOUNT EXPERIMENT_NAME=$EXPERIMENT_NAME AWS_INSTANCE_COMMAND=$DAS4_NODE_COMMAND AWS_NODE_TIMEOUT=$AWS_NODE_TIMEOUT SYNC_PORT=$SYNC_PORT SCENARIO_FILE=$SCENARIO_FILE TRACKER_PORT=$TRACKER_PORT TRIBLER_DIR=/home/ubuntu/tribler EXPERIMENT_DIR=/home/ubuntu/gumby/experiments/noodle VENV=/home/ubuntu/venv3 TX_RATE=$GUMBY_TX_RATE TX_SPAWN_DURATION=$GUMBY_TX_SPAWN_DURATION TX_GRACE_PERIOD=$GUMBY_TX_GRACE_PERIOD RISK=$GUMBY_RISK SYNC_TIME=$GUMBY_SYNC_TIME NUM_WIT=$GUMBY_NUM_WIT AUDIT_ON=$GUMBY_AUDIT_ON gumby/scripts/aws_run.sh"
  ssh -n ubuntu@$SERVER -i ~/Amazon.pem $cmd &
  pids[${i}]=$!
done < "$AWS_SERVERS_FILE"

# wait for all pids
for pid in ${pids[*]}; do
    wait $pid
done

# Rsync everything back
OUTPUT_DIR=/tmp/Experiment_${EXPERIMENT_NAME}_output
while read -r SERVER
do
  echo "RSynching back from $SERVER"
  mkdir -p output/localhost
  rsync -r ubuntu@$SERVER:$OUTPUT_DIR/ output/localhost/$SERVER -e "ssh -i ~/Amazon.pem"
done < "$AWS_SERVERS_FILE"
