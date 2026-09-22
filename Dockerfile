# syntax=docker/dockerfile:1

FROM golang:1.25-alpine AS build
WORKDIR /src

COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/source-reader ./cmd/source-reader \
    && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/destination-writer ./cmd/destination-writer

FROM alpine:3.22
RUN apk add --no-cache ca-certificates \
    && addgroup -S app \
    && adduser -S -G app app

COPY --from=build /out/source-reader /usr/local/bin/source-reader
COPY --from=build /out/destination-writer /usr/local/bin/destination-writer

USER app
CMD ["/usr/local/bin/source-reader"]
