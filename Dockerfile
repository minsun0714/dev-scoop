FROM openjdk:21-jdk-slim

# netcat 설치
RUN apt-get update && apt-get install -y netcat-openbsd && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY build/libs/*.jar app.jar
COPY wait-for-it.sh /wait-for-it.sh

RUN chmod +x /wait-for-it.sh

EXPOSE 8080
ENTRYPOINT ["/wait-for-it.sh", "elasticsearch:9200", "--", "java", "-jar", "app.jar"]
