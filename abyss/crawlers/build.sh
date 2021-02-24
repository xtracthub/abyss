#!/bin/bash
sudo docker build --build-arg aws_secret=$aws_secret --build-arg aws_access=$aws_access --build-arg transfer_token=$transfer_token -t globus-crawler .