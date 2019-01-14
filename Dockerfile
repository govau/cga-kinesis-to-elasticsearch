FROM golang:alpine AS builder

COPY . /go/src/github.com/govau/cga-kinesis-to-elasticsearch

# If we don't disable CGO, the binary won't work in the scratch image. Unsure why?
RUN CGO_ENABLED=0 go install github.com/govau/cga-kinesis-to-elasticsearch

FROM scratch

COPY --from=builder /go/bin/cga-kinesis-to-elasticsearch /go/bin/cga-kinesis-to-elasticsearch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

COPY index-mappings-logMessage.json /index-mappings-logMessage.json

ENTRYPOINT ["/go/bin/cga-kinesis-to-elasticsearch"]
