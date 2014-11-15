FROM bbc:wheezy
MAINTAINER Bertrand Bonnefoy-Claudet <bertrandbc@gmail.com>

# Environment
COPY dist /var/local/porygon/dist
WORKDIR /var/local/porygon
RUN ./dist/debian && apt-get update
RUN apt-get install -y -t jessie python3 python3-pip
RUN pip3 install python-slugify flask

# Installation
COPY . /var/local/porygon
