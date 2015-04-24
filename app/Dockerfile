FROM bbc2/debian-s6:jessie
MAINTAINER Bertrand Bonnefoy-Claudet <bertrandbc@gmail.com>

RUN apt-wrap apt-get update \
    && apt-wrap apt-get install -y --no-install-recommends python3 python3-pip python3-arrow \
    && pip3 install Flask python-slugify \
    && mkdir -p /var/local/porygon \

    # Clean up
    && apt-wrap apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY . /srv/porygon

COPY rootfs /

EXPOSE 5000
