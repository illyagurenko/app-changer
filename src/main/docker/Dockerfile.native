FROM registry.access.redhat.com/ubi9/ubi-minimal:9.5

WORKDIR /work/

RUN mkdir -p /work/logs \
    && chown -R 1001:root /work \
    && chmod -R "g+rwX" /work

COPY --chown=1001:root build/*-runner /work/application

RUN chmod +x /work/application

EXPOSE 8080
USER 1001

ENTRYPOINT ["./application", "-Dquarkus.http.host=0.0.0.0"]
