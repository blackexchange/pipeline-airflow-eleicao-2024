FROM apache/airflow:2.10.2

# Instalar o JDK necessário para o PySpark (usando OpenJDK 17)
USER root
RUN apt-get update && apt-get install -y openjdk-17-jdk-headless

# Definir a variável JAVA_HOME para o OpenJDK 17
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Garantir que o diretório /opt/airflow/out exista e tenha permissões adequadas
RUN mkdir -p /opt/airflow/out && chown -R airflow:root /opt/airflow/out
RUN mkdir -p /opt/airflow/in && chown -R airflow:root /opt/airflow/in


# Mudar para o usuário airflow para instalar o PySpark
USER airflow

# Instalar o PySpark como usuário airflow
RUN pip install --no-cache-dir pyspark

# Definir o diretório de trabalho para o Airflow
WORKDIR /opt/airflow

# Expor a porta do Airflow Webserver
EXPOSE 8080
