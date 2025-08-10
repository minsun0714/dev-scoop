# -----------------------
# 1단계: Gradle 빌드
# -----------------------
FROM gradle:8.8-jdk21 AS builder

WORKDIR /workspace
COPY . .

# 캐시 최적화를 위해 의존성 먼저 받기
RUN gradle dependencies --no-daemon || true

# 애플리케이션 빌드 (테스트 제외 가능)
RUN ./gradlew clean build --no-daemon

# -----------------------
# 2단계: 런타임 이미지
# -----------------------
FROM openjdk:21-jdk-slim

# netcat 설치
RUN apt-get update && apt-get install -y netcat-openbsd && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 1단계에서 빌드한 jar 복사
COPY --from=builder /workspace/build/libs/*.jar app.jar

# wait-for-it.sh 복사 및 실행 권한
COPY wait-for-it.sh /wait-for-it.sh
RUN chmod +x /wait-for-it.sh

EXPOSE 8080
ENTRYPOINT ["/wait-for-it.sh", "elasticsearch:9200", "java", "-jar", "app.jar"]
