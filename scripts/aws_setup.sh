#!/bin/bash

# rsync the entire Gumby repository to the AWS instances
while read -r SERVER
do
  echo "Sending Gumby directory to $SERVER..."
  rsync -r gumby ec2-user@$SERVER:/home/ec2-user -e "ssh -i ~/Amazon.pem"

  echo "Building virtualenv on $SERVER..."
  ssh -n ec2-user@$SERVER -i ~/Amazon.pem gumby/scripts/build_virtualenv_aws.sh
done < "$AWS_SERVERS_FILE"
