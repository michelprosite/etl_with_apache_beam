FROM python:3.10-slim
COPY --from=apache/beam_python3.10_sdk:2.55.0 /opt/apache/beam /opt/apache/beam
COPY --from=gcr.io/dataflow-templates-base/python310-template-launcher-base:20230622_RC00 /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher

# Diretório de trabalho
ARG WORKDIR=/template
WORKDIR ${WORKDIR}

# Copiando arquivos
COPY requirements.txt .
COPY setup.py .
COPY main.py .
COPY metadata.json .
COPY functions functions

RUN pip install --no-cache-dir -r requirements.txt

RUN pip install -e .

RUN chmod 777 -R ./*

# Instalando dependências
WORKDIR /opt
RUN apt-get update \
    && pip install --upgrade pip \
    && pip install apache-beam[gcp]==2.55.0 \
    # Upgrade pip e instalação de requisitos
    # Limpeza de cache
    && apt-get clean \
    && rm -rf /tmp/*

# Definição de variáveis de ambiente
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/main.py"
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="${WORKDIR}/setup.py"

RUN pip check

RUN pip freeze

ENTRYPOINT ["/opt/apache/beam/boot"]
