FROM bbc2/base

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && apt-get install -y -q python3 python3-pip \
    && pip3 install pyftpdlib

VOLUME /srv/ftp/

COPY server.py /usr/local/bin/server.py

COPY rootfs /

ENTRYPOINT ["/usr/bin/s6-svscan", "/etc/s6"]
CMD []
