# Etapa 1: compilar con Maven (no hace falta Java ni Maven en el equipo)
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B package -DskipTests

# Etapa 2: imagen de ejecucion (solo JRE)
FROM eclipse-temurin:21-jre
WORKDIR /app
# Agente JMX -> Prometheus. Se activa solo donde se define JAVA_TOOL_OPTIONS (servicio streams)
# y se copia a un volumen para los brokers y ZooKeeper (servicio jmx-agent).
ADD https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/1.0.1/jmx_prometheus_javaagent-1.0.1.jar /opt/jmx/jmx_prometheus_javaagent.jar
RUN chmod 644 /opt/jmx/jmx_prometheus_javaagent.jar
COPY --from=build /src/target/app.jar /app/app.jar
CMD ["java", "-cp", "/app/app.jar", "cl.duoc.ventas.StreamsApp"]
