FROM prom/prometheus as prom
FROM grafana/grafana as grafana
FROM prom/node-exporter as node-exporter

# build UI
FROM node:16-alpine3.15 as js-builder
ENV NODE_OPTIONS="--max_old_space_size=8000"

WORKDIR /ui

COPY ./ui .

ENV NODE_ENV production
RUN npm set progress=false; \
    npm install --omit=dev; \
    npm run build


# final stage
FROM python:3.9-slim
MAINTAINER openGauss AI-SIG <ai@opengauss.org>

WORKDIR /app

COPY requirements-x86.txt requirements.txt
COPY . .

COPY --from=prom /bin/prometheus /bin/prometheus
COPY --from=prom /etc/prometheus/prometheus.yml /etc/prometheus/prometheus.yml
COPY --from=prom /usr/share/prometheus/console_libraries/ /usr/share/prometheus/console_libraries/
COPY --from=prom /usr/share/prometheus/consoles/ /usr/share/prometheus/consoles/
COPY --from=node-exporter /bin/node_exporter /tmp/node_exporter

COPY --from=js-builder /ui/build /app/ui/build

# Install 3rd dependencies and move python runtime.
RUN pip install --no-cache-dir -r requirements.txt -t 3rd; \
    mkdir python; \
    mv /usr/local/bin python; \
    mv /usr/local/include python; \
    mv /usr/local/lib python

# Set envrionment for python runtime.
ENV PATH "$PATH:/app:/app/python/bin"
ENV LD_LIBRARY_PATH "/app/python/lib"
ENV PYTHONPATH "3rd"
# Prevent startup process from exiting, causing 
# the entire Docker container process to exit.
ENV DBMIND_USE_DAEMON "0"

# DBMind service
EXPOSE 8080
# Prometheus
EXPOSE 9090
# Grafana
EXPOSE 3000

CMD ["/app/python/bin/python", "/app/docker_run.py"]

