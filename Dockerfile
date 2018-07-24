FROM golang:1.8.3 as builder

RUN mkdir -p /go/src/github.com/EVODelavega/goq
ADD . /go/src/github.com/EVODelavega/goq

WORKDIR /go/src/github.com/EVODelavega/goq
RUN CGO_ENABLED=0 go install -a github.com/EVODelavega/goq/cmd/goq

FROM scratch

COPY --from=builder /go/bin/goq .
ENTRYPOINT ["./goq"]
