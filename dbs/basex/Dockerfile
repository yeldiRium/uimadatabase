FROM ubuntu:xenial

RUN apt-get update
RUN apt-get install -y basex

RUN adduser --home /srv --disabled-password --disabled-login --uid 1984 basex && chown -R basex /srv
USER basex

VOLUME data:/srv/BaseXData

EXPOSE 1984
CMD ["basexserver", "-nbasex"]