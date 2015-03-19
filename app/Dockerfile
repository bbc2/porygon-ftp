FROM bbc2/base
MAINTAINER Bertrand Bonnefoy-Claudet <bertrandbc@gmail.com>

RUN apt-get update && apt-get install -y python3 python3-pip \
    && pip3 install python-slugify flask \
    && mkdir -p /var/local/porygon

COPY . /srv/porygon

COPY rootfs /

EXPOSE 5000

ENTRYPOINT ["/usr/bin/s6-svscan", "/etc/s6"]
CMD []
