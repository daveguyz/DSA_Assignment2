# ---------- Stage 1: compile the Ballerina package to an executable JAR ----------
FROM ballerina/ballerina:2201.10.3 AS build
USER root
WORKDIR /src
COPY Ballerina.toml ./
COPY *.bal ./
RUN bal build

# ---------- Stage 2: slim runtime (Ballerina 2201.10.x runs on Java 17) ----------
FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
COPY --from=build /src/target/bin/customer_service.jar /app/service.jar
COPY Config.docker.toml /app/Config.toml
ENV JAVA_TOOL_OPTIONS="-Xms64m -Xmx256m -XX:+UseSerialGC"
EXPOSE 9091 9797
HEALTHCHECK --interval=15s --timeout=3s --start-period=60s --retries=5 \
  CMD curl -fs http://localhost:9091/health || exit 1
ENTRYPOINT ["java", "-jar", "/app/service.jar"]
