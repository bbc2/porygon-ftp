FROM bbc2/debian-s6:jessie

RUN apt-wrap apt-get update \
    && apt-wrap apt-get install -y --no-install-recommends python3 python3-pip \
    && pip3 install pyftpdlib \

    # Clean up
    && apt-wrap apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

VOLUME /srv/ftp/

COPY server.py /usr/local/bin/server.py

COPY rootfs /
