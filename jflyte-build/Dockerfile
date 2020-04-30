FROM gcr.io/distroless/java:8

COPY target/plugins /jflyte-plugins
COPY target/jflyte.jar /jflyte.jar

ENV FLYTE_INTERNAL_PLUGIN_DIR "/jflyte-plugins"

ENV FLYTE_PLATFORM_URL "CHANGEME"
ENV FLYTE_INTERNAL_IMAGE "CHANGEME"
ENV FLYTE_STAGING_LOCATION "CHANGEME"
ENV FLYTE_PLATFORM_INSECURE "False"

ENTRYPOINT ["java", "-Dorg.slf4j.simpleLogger.defaultLogLevel=INFO", "-Dorg.slf4j.simpleLogger.log.org.flyte=DEBUG", "-cp", "/jflyte.jar", "org.flyte.jflyte.Main"]
