FROM apache/superset:2.1.0

USER root

# Instalujemy tylko absolutne minimum dla systemu
RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Instalujemy tylko sterownik do Trino
RUN pip install --no-cache-dir \
    trino \
    sqlalchemy-trino

USER superset