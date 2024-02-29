FROM mambaorg/micromamba:1.5.6-alpine3.19

USER root
RUN addgroup -S cyano && adduser -S -G cyano cyano

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8
ENV PATH /opt/conda/bin:/opt/conda/envs/env/bin:/opt/micromamba/bin:/opt/micromamba/envs/env/bin:$PATH

RUN apk add --upgrade apk-tools
RUN apk upgrade --available

RUN apk add wget bzip2 ca-certificates \
    py3-pip make sqlite gfortran git \
    mercurial subversion gdal geos
RUN apk upgrade --available

ARG CONDA_ENV="base"
ARG GDAL_VERSION=3.7.1

USER cyano

COPY environment.yml /src/environment.yml
RUN micromamba install -n $CONDA_ENV -f /src/environment.yml
RUN micromamba clean -p -t -l --trash -y
RUN pip uninstall -y xhtml2pdf && pip install xhtml2pdf

COPY . /src/

USER root
RUN chmod 755 /src/start_flask.sh
COPY uwsgi.ini /etc/uwsgi/uwsgi.ini

WORKDIR /src
EXPOSE 8080
RUN chown -R cyano:cyano /src

# Security Issues Mitigations
# ------------------------- #
RUN apk del gfortran
RUN rm -R /opt/conda/pkgs/redis*
RUN rm -R /opt/conda/bin/redis*
RUN rm -R /opt/conda/pkgs/postgres*
RUN rm -R /opt/conda/bin/postgres*
RUN find /opt/conda/pkgs/future* -name "*.pem" -delete || true
RUN find /opt/conda/lib/python3.10/site-packages/future -name "*.pem" -delete || true
RUN find /opt/conda -name "*test.key" -delete || true
RUN find /opt/conda/ -name 'test.key' -delete || true
RUN find /opt/conda/ -name 'localhost.key' -delete || true
RUN find /opt/conda/ -name 'server.pem' -delete || true
RUN find /opt/conda/ -name 'client.pem' -delete || true
RUN find /opt/conda/ -name 'password_protected.pem' -delete || true
# ------------------------- #
USER cyano

#ENTRYPOINT ["tail", "-f", "/dev/null"]
CMD ["sh", "start_flask.sh"]
