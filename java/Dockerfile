FROM maven:3.9-eclipse-temurin-11 as build
WORKDIR /application
COPY pom.xml .
COPY src src
RUN mvn -B clean package -DskipTests -Dmaven.test.skip=true
FROM eclipse-temurin:11-jre as runtime
COPY --from=build /application/target/*.jar app.jar
EXPOSE 9000
ENTRYPOINT ["java", "-jar", "/app.jar"]
