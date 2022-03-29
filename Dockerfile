FROM ubuntu:20.04
MAINTAINER cedfactory
LABEL version="0.1"

RUN apt-get update
RUN apt-get install -y wget python3-pip
#RUN pip install --upgrade pip

COPY requirements.txt /
RUN pip install -r requirements.txt

WORKDIR "/mnt"
