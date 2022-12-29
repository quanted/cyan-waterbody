FROM continuumio/miniconda3:4.12.0 as base

ENV DEBIAN_FRONTEND noninteractive

ARG GDAL_VERSION=3.4.1
ARG CONDA_ENV_BASE=pyenv

RUN apt-get -y update
RUN apt-get -y install libgeos-dev python3-pip python3-gdal cmake git gfortran sqlite3 build-essential
RUN pip install -U pip

RUN conda config --add channels conda-forge
RUN conda create -n $CONDA_ENV_BASE python=3.9.10 gdal=$GDAL_VERSION uwsgi

RUN conda install -n $CONDA_ENV_BASE fiona geopandas
COPY requirements.txt /tmp/requirements.txt
RUN conda run -n $CONDA_ENV_BASE --no-capture-output pip3 install -r /tmp/requirements.txt

RUN conda run -n $CONDA_ENV_BASE --no-capture-output conda clean -acfy && \
    find /opt/conda -follow -type f -name '*.a' -delete && \
    find /opt/conda -follow -type f -name '*.pyc' -delete && \
    find /opt/conda -follow -type f -name '*.js.map' -delete

# Main image build
FROM continuumio/miniconda3:4.12.0 as prime

ENV DEBIAN_FRONTEND noninteractive

ENV APP_USER=www-data
ENV CONDA_ENV=/opt/conda/envs/pyenv

RUN apt-get -y update
RUN apt-get -y install libgeos-dev python3-gdal cmake git gfortran sqlite3

COPY uwsgi.ini /etc/uwsgi/
COPY . /src/cyan_waterbody/
RUN chmod 755 /src/cyan_waterbody/start_flask.sh
WORKDIR /src/
EXPOSE 8080

COPY --from=base /opt/conda/envs/pyenv $CONDA_ENV

ENV PYTHONPATH /src:/src/cyan_waterbody/:$CONDA_ENV:$PYTHONPATH
ENV PATH /src:/src/cyan_waterbody/:$CONDA_ENV:$PATH

#RUN chown -R $APP_USER:$APP_USER /src
#RUN chown $APP_USER:$APP_USER $CONDA_ENV
#USER $APP_USER

CMD ["conda", "run", "-p", "$CONDA_ENV", "--no-capture-output", "sh", "/src/cyan_waterbody/start_flask.sh"]