# syntax = docker/dockerfile:experimental
# Build Container
FROM golang:1.20 as builder

ENV GO111MODULE on
ENV GOPRIVATE=github.com/latonaio
ENV CGO_ENABLED=0
WORKDIR /go/src/github.com/latonaio

COPY . .
RUN go mod download
RUN go build -o convert-to-dpfm-orders-from-orders-edi-for-smes ./

# Runtime Container
FROM alpine:3.14
RUN apk add --no-cache libc6-compat
ENV SERVICE=convert-to-dpfm-orders-from-orders-edi-for-smes \
    APP_DIR="${AION_HOME}/${POSITION}/${SERVICE}"

WORKDIR ${AION_HOME}

COPY --from=builder /go/src/github.com/latonaio/convert-to-dpfm-orders-from-orders-edi-for-smes .

CMD ["./convert-to-dpfm-orders-from-orders-edi-for-smes"]
