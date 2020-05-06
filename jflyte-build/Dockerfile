FROM gcr.io/distroless/java:8

COPY target/lib /jflyte
COPY target/plugins /jflyte/plugins/

ENV FLYTE_INTERNAL_PLUGIN_DIR "/jflyte/plugins"

ENV FLYTE_PLATFORM_URL "CHANGEME"
ENV FLYTE_INTERNAL_IMAGE "CHANGEME"
ENV FLYTE_STAGING_LOCATION "CHANGEME"
ENV FLYTE_PLATFORM_INSECURE "False"

ENTRYPOINT ["java", "-Dorg.slf4j.simpleLogger.defaultLogLevel=INFO", "-Dorg.slf4j.simpleLogger.log.org.flyte=DEBUG", "-cp", "/jflyte/*", "org.flyte.jflyte.Main"]
