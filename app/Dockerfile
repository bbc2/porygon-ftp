FROM bbc2/base
MAINTAINER Bertrand Bonnefoy-Claudet <bertrandbc@gmail.com>

RUN apt-get update && apt-get install -y python3 python3-pip python3-arrow \
    && pip3 install Flask python-slugify \
    && mkdir -p /var/local/porygon

COPY . /srv/porygon

COPY rootfs /

EXPOSE 5000

ENTRYPOINT ["/usr/bin/s6-svscan", "/etc/s6"]
CMD []
