FROM python:3.12-slim

# システム依存関係のインストール
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    git \
    gnupg \
    software-properties-common \
    lsb-release \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# HashiCorp リポジトリの設定
RUN wget -O- https://apt.releases.hashicorp.com/gpg | \
    gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] \
    https://apt.releases.hashicorp.com $(lsb_release -cs) main" > /etc/apt/sources.list.d/hashicorp.list

# Terraform のインストール
RUN apt-get update && apt-get install -y terraform \
    && rm -rf /var/lib/apt/lists/*

# Terragrunt のインストール
ARG TERRAGRUNT_VERSION=latest
RUN wget -O /usr/local/bin/terragrunt \
    "https://github.com/gruntwork-io/terragrunt/releases/${TERRAGRUNT_VERSION}/download/terragrunt_linux_amd64" \
    && chmod +x /usr/local/bin/terragrunt

# Python 依存関係のインストール
WORKDIR /workspace
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && rm requirements.txt

ENV PYTHONUNBUFFERED=1

CMD ["/bin/bash"]