FROM mambaorg/micromamba:1.5.1-alpine

USER root
RUN addgroup -S cyano && adduser -S -G cyano cyano

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8
ENV PATH /opt/conda/bin:/opt/conda/envs/env/bin:/opt/micromamba/bin:/opt/micromamba/envs/env/bin:$PATH

RUN apk add --upgrade apk-tools
RUN apk upgrade --available

RUN apk add wget bzip2 ca-certificates \
    py3-pip make sqlite gfortran git \
    mercurial subversion gdal geos

ARG CONDA_ENV="base"
ARG GDAL_VERSION=3.7.1

COPY environment.yml /src/environment.yml
RUN micromamba install -n $CONDA_ENV -f /src/environment.yml

COPY . /src/

RUN chmod 755 /src/start_flask.sh
RUN micromamba clean --all --yes

COPY uwsgi.ini /etc/uwsgi/uwsgi.ini

WORKDIR /src
EXPOSE 8080
RUN chown -R cyano:cyano /src

USER cyano

#ENTRYPOINT ["tail", "-f", "/dev/null"]
CMD ["sh", "start_flask.sh"]