FROM python:3.7

RUN apt-get update

RUN git clone --single-branch --branch crawler_container https://github.com/xtracthub/abyss /abyss

WORKDIR /abyss

RUN pip3 install -r requirements.txt

ARG aws_secret=aws_secret
ARG aws_access=aws_access
ARG transfer_token=transfer_token

ENV AWS_SECRET=$aws_secret
ENV AWS_ACCESS=$aws_access
ENV TRANSFER_TOKEN=$transfer_token


CMD ["/bin/bash"]