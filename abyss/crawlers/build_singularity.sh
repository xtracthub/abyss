#!/bin/bash
docker build --build-arg aws_secret=$AWS_SECRET --build-arg aws_access=$AWS_ACCESS --build-arg transfer_token=$TRANSFER_TOKEN -t globus-crawler .
docker save globus-crawler -o globus-crawler.tar
singularity build globus-crawler.sif docker-archive://globus-crawler.tar