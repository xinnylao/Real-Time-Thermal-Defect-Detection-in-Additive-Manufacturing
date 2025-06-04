# DEBS Challenge - Flink Pipeline


Welcome to our project!:) You will see the whole pipeline and can reproduce the result by following this document:D
This project is a streaming data processing pipeline using **Apache Flink**.

We have two different versions of our code (refer to the design document), the folder indicates which version you are running.

## Table of Contents
- [1. Prerequisites](#1-prerequisites)
- [2. Setup & Start Services](#2-setup--start-services)
   - [2.1 Start Docker Container](#21-start-docker-container)
   - [2.2 Start Flink Cluster](#22-start-flink-cluster)
- [3. Run the Pipeline](#3-run-the-pipeline)
   - [3.1 Build the .jar File](#31-build-the-jar-file)
   - [3.2 Run the Flink Job](#32-run-the-flink-job)
- [4. Shutdown Services](#4-shutdown-services)

---

## 1. Prerequisites
Before running the pipeline, make sure you have installed:

- **Java 11**
- **Apache Flink** (`flink-1.20.0`)
- **Apache Maven and its related dependency**
---

## 2. Setup & Start Services
### 2.1 Start Docker Container
 
```sh
docker image load -i gc25cdocker.tar
docker run -p 8866:8866 -v ./data:/data micro-challenger:latest 0.0.0.0:8866 /data
```

Check that the Docker container is running in Docker Desktop.

### 2.2 Start Flink Cluster
Start the Flink job manager and task manager:

```sh
start-cluster.sh
```

Start multiple clusters to have enough task managers for the job. 
Once Flink is running, access the web UI at: http://localhost:8081

---

## 3. Run the Pipeline
The Flink job fetches batches from API endpoint and processes it. 
### 3.1 Build the .jar File
If you haven't built the project jar file yet or if you make any changes to BenchmarkLauncher.jar and other classes it relies on,
(re)build the jar file by running the command in the project directory:

```shell
mvn clean package
```
This will (re)build the project and generate a BenchmarkLauncher.jar under the target directory.

### 3.2 Run the Flink Job

Now we can deploy our flink job using:

```sh
flink run target/BenchmarkLauncher.jar
```

---

## 4. Shutdown Services
Stop Flink:

```sh
stop-cluster.sh
```

Make sure to stop all Flink task managers. 
