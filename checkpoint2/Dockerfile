ARG MELTANO_IMAGE=meltano/meltano:latest-python3.11
FROM $MELTANO_IMAGE

RUN apt update && \
    apt install -y curl && \
    apt install -y unzip

RUN curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

WORKDIR /projects

COPY ./meltano_API/ ./meltano_API/
COPY ./meltano_DB/ ./meltano_DB/

RUN touch ./meltano_API/README.md

RUN echo "Instalando dependências do projeto API..." && \
    (cd meltano_API && meltano install)

RUN echo "Instalando dependências do projeto DB..." && \
    (cd meltano_DB && meltano install)

COPY ./entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

ENTRYPOINT ["entrypoint.sh"]