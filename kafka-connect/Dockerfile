FROM quay.io/debezium/connect:3.5.2

USER root
ADD https://github.com/prometheus/jmx_exporter/releases/download/1.5.0/jmx_prometheus_javaagent-1.5.0.jar /opt/jmx/jmx_prometheus_javaagent.jar
RUN chmod 0444 /opt/jmx/jmx_prometheus_javaagent.jar
USER kafka

