# Dockerfile
FROM flink:1.19.1-scala_2.12
COPY target/BenchmarkLauncher.jar /opt/flink/usrlib/
ENTRYPOINT ["/docker-entrypoint.sh", "standalone-job"]
CMD ["--job-classname", "MainController"]