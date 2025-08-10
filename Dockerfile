# 1단계: 빌드 스테이지
FROM gradle:8.8-jdk21 AS builder
WORKDIR /workspace
COPY . .
RUN ./gradlew clean build --no-daemon -x test

# 2단계: 실행 스테이지
FROM openjdk:21-jdk-slim
RUN apt-get update && apt-get install -y netcat-openbsd && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /workspace/build/libs/*.jar app.jar
COPY wait-for-it.sh /wait-for-it.sh
RUN chmod +x /wait-for-it.sh
EXPOSE 8080
ENTRYPOINT ["/wait-for-it.sh", "elasticsearch:9200", "java", "-jar", "app.jar"]
