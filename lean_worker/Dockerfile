FROM golang:1.24.1 AS builder
# Client uses docker multistage builds feature https://docs.docker.com/develop/develop-images/multistage-build/
# First stage is used to compile golang binary and second stage is used to only copy the
# binary generated to the deploy image.
# Docker multi stage does not delete intermediate stages used to build our image, so we need
# to delete it by ourselves. Since docker does not give a good alternative to delete the intermediate images
# we are adding a very specific label to the image to then find these kind of images and delete them
LABEL intermediateStageToBeDeleted=true

RUN mkdir -p /build/common
RUN mkdir -p /build/middleware
RUN mkdir -p /build/worker
RUN mkdir -p /build/lean_worker
WORKDIR /build/lean_worker

COPY ./common /build/common
COPY ./middleware /build/middleware
COPY ./worker /build/worker
COPY ./lean_worker/go.mod ./lean_worker/go.sum ./
RUN go mod download

COPY ./lean_worker .

RUN go test -v ./...

# CGO_ENABLED must be disabled to run go binary in Alpine
RUN CGO_ENABLED=0 GOOS=linux go build -o /build/bin/lean_worker github.com/ptourne/sistemas-distribuidos-1/lean_worker

FROM busybox:latest
COPY --from=builder /build/bin/lean_worker /lean_worker
ENTRYPOINT ["/bin/sh"]
