#!/bin/bash
exec uwsgi --plugins http,python3 -H /opt/conda/envs/pyenv /etc/uwsgi/uwsgi.ini