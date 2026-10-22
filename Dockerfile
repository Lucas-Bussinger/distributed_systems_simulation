# Usa a imagem oficial do Python como base
FROM python:3.10-slim

# Evita buffering de logs em stdout/stderr no Python
ENV PYTHONUNBUFFERED=1

# Instala ferramentas úteis de rede para diagnóstico e testes interativos
RUN apt-get update && apt-get install -y --no-install-recommends \
    iputils-ping \
    dnsutils \
    procps \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Define o diretório de trabalho padrão
WORKDIR /app

# Copia o arquivo de requisitos para dentro da imagem
COPY requirements.txt .

# Instala as bibliotecas Python (gRPC, protobuf)
RUN pip install --no-cache-dir -r requirements.txt

# Executa o nó do algoritmo distribuído
CMD ["python", "-u", "node.py"]