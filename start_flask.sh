#!/bin/bash
exec uwsgi --plugins http,python3 /etc/uwsgi/uwsgi.ini