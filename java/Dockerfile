# Build app from Maven image
FROM maven:3.8.4-openjdk-11 as build

# Set the working directory in the Docker image filesystem.
WORKDIR /application

# Copy the Maven pom.xml file into the image to cache dependencies
COPY pom.xml .

# Copy the source code into the image
COPY src src

# Package the application
RUN mvn -B clean package -DskipTests -Dmaven.test.skip=true

# Use the official OpenJDK image to run the application
FROM openjdk:11-jre-slim as runtime

# Copy the built application JAR from the build stage to the runtime stage
COPY --from=build /application/target/*.jar app.jar

# Expose the port the application runs on
EXPOSE 9000

# Run the app
ENTRYPOINT ["java", "-jar", "/app.jar"]
