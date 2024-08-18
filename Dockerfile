# syntax=docker/dockerfile:1
FROM golang:1.23 AS build

WORKDIR /app
COPY . .

RUN go mod download

# Build
RUN CGO_ENABLED=0 GOOS=linux go build -o /go-message-queue

FROM gcr.io/distroless/base
COPY --from=build /go-message-queue /go-message-queue
CMD ["/go-message-queue"]