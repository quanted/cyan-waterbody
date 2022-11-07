FROM continuumio/miniconda3:4.10.3p0-alpine as base

ARG CONDA_ENV_BASE=pyenv

RUN apk update
RUN apk upgrade
RUN apk add --no-cache geos gdal cmake git gfortran sqlite sqlite-dev
RUN pip install -U pip

COPY environment.yml /tmp/environment.yml

RUN conda config --add channels conda-forge
RUN conda env create -n $CONDA_ENV_BASE --file /tmp/environment.yml
RUN conda install -n $CONDA_ENV_BASE uwsgi
RUN conda install --force-reinstall -n $CONDA_ENV_BASE gdal
RUN conda install --force-reinstall -n $CONDA_ENV_BASE fiona
RUN conda install --force-reinstall -n $CONDA_ENV_BASE geopandas

RUN conda run -n $CONDA_ENV_BASE --no-capture-output conda clean -acfy && \
    find /opt/conda -follow -type f -name '*.a' -delete && \
    find /opt/conda -follow -type f -name '*.pyc' -delete && \
    find /opt/conda -follow -type f -name '*.js.map' -delete

FROM continuumio/miniconda3:4.10.3p0-alpine as prime

ENV APP_USER=www-data
ENV CONDA_ENV=/opt/conda/envs/pyenv

RUN adduser -S $APP_USER -G $APP_USER

RUN apk update
RUN apk upgrade
RUN apk add --no-cache geos gdal sqlite sqlite-dev

COPY uwsgi.ini /etc/uwsgi/
COPY . /src/cyan_waterbody
WORKDIR /src/
EXPOSE 8080

COPY --from=base /opt/conda/envs/pyenv $CONDA_ENV

ENV PYTHONPATH $CONDA_ENV:/src:/src/cyan_waterbody/:$PYTHONPATH
ENV PATH $CONDA_ENV:/src:/src/cyan_waterbody/:$PATH

RUN chown $APP_USER:$APP_USER /src/
RUN chown $APP_USER:$APP_USER $CONDA_ENV
USER $APP_USER

CMD ["conda", "run", "-p", "$CONDA_ENV", "--no-capture-output", "python", "wb_flask.py"]