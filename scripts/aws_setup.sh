#!/bin/bash

echo "Syncing Gumby directory with instances"
while read -r SERVER
do
  echo "Sending Gumby directory to $SERVER..."
  rsync -r gumby ubuntu@$SERVER:/home/ubuntu -e "ssh -i ~/Amazon.pem" &
  pids[${i}]=$!
done < "$AWS_SERVERS_FILE"

# wait for all pids
for pid in ${pids[*]}; do
    wait $pid
done

echo "Syncing Tribler directory to instances"
while read -r SERVER
do
  echo "Sending Tribler directory to $SERVER..."
  rsync -r tribler ubuntu@$SERVER:/home/ubuntu -e "ssh -i ~/Amazon.pem" &
  pids[${i}]=$!
done < "$AWS_SERVERS_FILE"

# wait for all pids
for pid in ${pids[*]}; do
    wait $pid
done

echo "Building venvs on instances"
while read -r SERVER
do
  echo "Building virtualenv on $SERVER..."
  ssh -n ubuntu@$SERVER -i ~/Amazon.pem gumby/scripts/build_virtualenv_aws.sh &
  pids[${i}]=$!
done < "$AWS_SERVERS_FILE"

# wait for all pids
for pid in ${pids[*]}; do
    wait $pid
done
