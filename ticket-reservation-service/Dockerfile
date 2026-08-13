# =========================================
# 1. Build Stage (Gradle + JDK17)
# =========================================
FROM gradle:8.10-jdk17 AS build
WORKDIR /app

# 의존성 캐시 최적화: 빌드 스크립트 먼저 복사
COPY build.gradle settings.gradle ./
RUN gradle dependencies --no-daemon || true

# 소스 복사 후 bootJar 빌드 (테스트는 이미지 빌드 시 스킵)
COPY src ./src
RUN gradle clean bootJar --no-daemon -x test

# =========================================
# 2. Runtime Stage (JRE 17)
# =========================================
FROM eclipse-temurin:17-jre AS runtime
WORKDIR /app

# 빌드 스테이지에서 생성된 부트 실행 jar만 복사
COPY --from=build /app/build/libs/*-SNAPSHOT.jar app.jar

# SERVER_PORT 환경변수로 오버라이드 (deployment 에서 8080 주입, 외부는 Service 가 8085 로 노출)
EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]
