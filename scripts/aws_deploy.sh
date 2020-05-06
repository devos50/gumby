#!/bin/bash

while read -r SERVER
do
  cmd="AWS_INSTANCES_TO_RUN=$AWS_INSTANCES_TO_RUN AWS_NODE_AMOUNT=$AWS_NODE_AMOUNT EXPERIMENT_NAME=$EXPERIMENT_NAME AWS_INSTANCE_COMMAND=$DAS4_NODE_COMMAND AWS_NODE_TIMEOUT=$AWS_NODE_TIMEOUT SYNC_PORT=$SYNC_PORT SCENARIO_FILE=$SCENARIO_FILE TRIBLER_DIR=/home/ec2-user/tribler EXPERIMENT_DIR=/home/ec2-user/gumby/experiments/noodle VENV=/home/ec2-user/venv3 gumby/scripts/aws_run.sh"
  ssh -n ec2-user@$SERVER -i ~/Amazon.pem $cmd &
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
  rsync -r ec2-user@$SERVER:$OUTPUT_DIR/ output/localhost/$SERVER -e "ssh -i ~/Amazon.pem"
done < "$AWS_SERVERS_FILE"
