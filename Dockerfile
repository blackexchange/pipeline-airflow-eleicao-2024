# Usar a imagem oficial do Airflow
FROM apache/airflow:2.10.2

# Mudar para o usuário root para instalar dependências do sistema
USER root

# Atualizar o apt e instalar o OpenJDK 17
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Definir a variável JAVA_HOME para o OpenJDK 17
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Criar diretórios necessários e ajustar permissões
RUN mkdir -p /opt/airflow/out /opt/airflow/in && \
    chown -R airflow:root /opt/airflow/out /opt/airflow/in

# Mudar para o usuário airflow para continuar a instalação
USER airflow

# Instalar o PySpark como usuário airflow
RUN pip install --no-cache-dir pyspark==3.5.3  # Ajuste para a versão que deseja

# Definir o diretório de trabalho para o Airflow
WORKDIR /opt/airflow

# Expor a porta do Airflow Webserver
EXPOSE 8080
