#!/bin/bash

source $VENV/bin/activate

let "PROCESSES_PER_NODE=$AWS_INSTANCES_TO_RUN/$AWS_NODE_AMOUNT"
let "PLUS_ONE_NODES=$AWS_INSTANCES_TO_RUN%$AWS_NODE_AMOUNT"

set -e

PROCESSES_IN_THIS_NODE=$PROCESSES_PER_NODE

echo "$(hostname) here, spawning $PROCESSES_IN_THIS_NODE instances of command: $AWS_INSTANCE_COMMAND"

OUTPUT_DIR=/tmp/Experiment_${EXPERIMENT_NAME}_output
rm -fR "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"
cd "$OUTPUT_DIR"

export OUTPUT_DIR

CMDFILE=/tmp/process_guard_XXXXXXXXXXXXX_$USER
echo "CMD file: $CMDFILE"

# @CONF_OPTION DAS4_NODE_COMMAND: The command that will be repeatedly launched in the worker nodes of the cluster. (required)
for INSTANCE in $(seq 1 1 $PROCESSES_IN_THIS_NODE); do
    echo "$AWS_INSTANCE_COMMAND" >> $CMDFILE
done

# Make sure gumby can be found
export PYTHONPATH=/home/ubuntu/gumby
export PATH=/home/ubuntu/gumby/gumby:$PATH

# @CONF_OPTION DAS4_NODE_TIMEOUT: Time in seconds to wait for the sub-processes to run before killing them. (required)
(process_guard.py -f $CMDFILE -t $AWS_NODE_TIMEOUT -o $OUTPUT_DIR -m $OUTPUT_DIR  -i 5 2>&1 | tee process_guard.log) ||:

rm $CMDFILE
