FROM quay.io/debezium/connect:2.7
# Debezium JDBC sink connector + Microsoft JDBC driver into the plugin path.
ENV KAFKA_CONNECT_PLUGINS_DIR=/kafka/connect
USER root
RUN mkdir -p /kafka/connect/debezium-jdbc && \
    curl -fSL -o /tmp/jdbc.tar.gz \
      https://repo1.maven.org/maven2/io/debezium/debezium-connector-jdbc/2.7.0.Final/debezium-connector-jdbc-2.7.0.Final-plugin.tar.gz && \
    tar -xzf /tmp/jdbc.tar.gz -C /kafka/connect/ && \
    curl -fSL -o /kafka/connect/debezium-connector-jdbc/mssql-jdbc.jar \
      https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.6.1.jre11/mssql-jdbc-12.6.1.jre11.jar
USER 1001
