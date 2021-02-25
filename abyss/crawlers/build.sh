#!/bin/bash
docker build --build-arg aws_secret=$AWS_SECRET --build-arg aws_access=$AWS_ACCESS --build-arg transfer_token=$TRANSFER_TOKEN -t globus-crawler .