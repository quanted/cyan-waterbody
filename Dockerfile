FROM continuumio/miniconda3:4.10.3p0-alpine as base

ARG CONDA_ENV_BASE=pyenv

RUN apk update
RUN apk upgrade
RUN apk add --no-cache gcc geos gdal cmake git gfortran sqlite sqlite-dev pcre-dev linux-headers libc-dev libffi-dev
RUN pip install -U pip

COPY conda_environment.yml /tmp/environment.yml

RUN conda config --add channels conda-forge
RUN conda create -n $CONDA_ENV_BASE python=3.8 gdal=3.6.1 geopandas rasterio flask fiona
RUN conda env update -n $CONDA_ENV_BASE --file /tmp/environment.yml

#RUN conda install -n $CONDA_ENV_BASE uwsgi
#RUN conda install --force-reinstall -n $CONDA_ENV_BASE gdal=3.5.3
#RUN conda install --force-reinstall -n $CONDA_ENV_BASE fiona=1.8.18
#RUN conda install --force-reinstall -n $CONDA_ENV_BASE geopandas=0.9.0

RUN conda run -n $CONDA_ENV_BASE --no-capture-output conda clean -acfy && \
    find /opt/conda -follow -type f -name '*.a' -delete && \
    find /opt/conda -follow -type f -name '*.pyc' -delete && \
    find /opt/conda -follow -type f -name '*.js.map' -delete

FROM continuumio/miniconda3:4.10.3p0-alpine as prime

ENV APP_USER=www-data
ENV CONDA_ENV=/opt/conda/envs/pyenv

RUN adduser -S $APP_USER -G $APP_USER -G root

RUN apk update
RUN apk upgrade
RUN apk add --no-cache gcc cmake geos gdal sqlite sqlite-dev wget curl \
    uwsgi uwsgi-http uwsgi-python3 \
    python3 python3-dev

COPY uwsgi.ini /etc/uwsgi/
COPY . /src/cyan_waterbody
RUN chmod 755 /src/cyan_waterbody/start_flask.sh
EXPOSE 8080
WORKDIR /src/

COPY --from=base /opt/conda/envs/pyenv $CONDA_ENV

ENV PYTHONPATH /src:/src/cyan_waterbody/:$CONDA_ENV:$PYTHONPATH
ENV PATH /src:/src/cyan_waterbody/:$CONDA_ENV:$PATH
ENV PYTHONHOME /opt/conda/envs/pyenv

#RUN chown -R $APP_USER:$APP_USER /src/
#RUN chown $APP_USER:$APP_USER $CONDA_ENV
#USER $APP_USER

CMD ["conda", "run", "-p", "$CONDA_ENV", "--no-capture-output", "sh", "/src/cyan_waterbody/start_flask.sh"]