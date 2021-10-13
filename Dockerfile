FROM scratch

COPY target/main /main
ENTRYPOINT [ "/main" ]
