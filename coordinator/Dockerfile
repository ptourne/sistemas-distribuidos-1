FROM golang:1.24.1 AS builder
# Client uses docker multistage builds feature https://docs.docker.com/develop/develop-images/multistage-build/
# First stage is used to compile golang binary and second stage is used to only copy the
# binary generated to the deploy image.
# Docker multi stage does not delete intermediate stages used to build our image, so we need
# to delete it by ourselves. Since docker does not give a good alternative to delete the intermediate images
# we are adding a very specific label to the image to then find these kind of images and delete them
LABEL intermediateStageToBeDeleted=true

RUN mkdir -p /build
WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY ../common ./common
COPY ./ ./coordinator
RUN go test -v ./...

# CGO_ENABLED must be disabled to run go binary in Alpine
RUN CGO_ENABLED=0 GOOS=linux go build -mod vendor -o bin/coordinator github.com/ptourne/sistemas-distribuidos-1/coordinator


FROM busybox:latest
COPY --from=builder /build/bin/coordinator /coordinator
ENTRYPOINT ["/bin/sh"]
