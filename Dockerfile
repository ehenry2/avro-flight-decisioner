FROM golang:alpine as builder

RUN mkdir /app
COPY . /app
RUN cd /app && \
    go build

FROM alpine
RUN mkdir /app
COPY --from=builder /app/avro-flight-decisioner /app/
CMD ["/app/avro-flight-decisioner"]
