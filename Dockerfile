# syntax=docker/dockerfile:1

# Build stage (multi-arch image, runs natively on Apple Silicon)
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY pom.xml .
COPY src ./src
# No dependency:go-offline. The cache mount keeps ~/.m2 between builds, so dependencies are
# downloaded once and the Maven output stays visible (no -q).
RUN --mount=type=cache,target=/root/.m2 mvn -B -DskipTests package

# Runtime stage
FROM eclipse-temurin:21-jre
WORKDIR /app
RUN useradd --system --no-create-home wallet
COPY --from=build /build/target/wallet-api-*.jar app.jar
USER wallet
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]