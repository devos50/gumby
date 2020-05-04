#!/bin/bash

cmd="AWS_INSTANCES_TO_RUN=$AWS_INSTANCES_TO_RUN AWS_NODE_AMOUNT=$AWS_NODE_AMOUNT EXPERIMENT_NAME=$EXPERIMENT_NAME AWS_INSTANCE_COMMAND=$DAS4_NODE_COMMAND AWS_NODE_TIMEOUT=$AWS_NODE_TIMEOUT SYNC_PORT=$SYNC_PORT SCENARIO_FILE=$SCENARIO_FILE EXPERIMENT_DIR=/home/ec2-user/gumby/experiments/dummy VENV=/home/ec2-user/venv3 gumby/scripts/aws_run.sh"
cat $AWS_SERVERS_FILE | xargs -I % ssh ec2-user@% -i ~/Amazon.pem $cmd

# Rsync everything back
echo "RSynching results back"
OUTPUT_DIR=/tmp/Experiment_${EXPERIMENT_NAME}_output
while read -r SERVER
do
  rsync -r ec2-user@$SERVER:$OUTPUT_DIR/ output/$SERVER -e "ssh -i ~/Amazon.pem"
done < "$AWS_SERVERS_FILE"
