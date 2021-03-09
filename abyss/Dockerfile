FROM python:3.7

RUN apt-get update

COPY . /abyss

WORKDIR /abyss

RUN pip3 install -r requirements.txt

RUN git clone --branch xtract-jetstream https://github.com/funcx-faas/funcX ~/funcX

RUN pip3 install ~/funcX/funcx_endpoint/
RUN pip3 install ~/funcX/funcx_sdk/


ARG aws_secret=aws_secret
ARG aws_access=aws_access
ARG transfer_token=transfer_token

ENV AWS_SECRET=$aws_secret
ENV AWS_ACCESS=$aws_access
ENV TRANSFER_TOKEN=$transfer_token


CMD ["/bin/bash"]
