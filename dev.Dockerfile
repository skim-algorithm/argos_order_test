FROM golang:1.15

RUN go get -u github.com/uudashr/gopkgs/v2/cmd/gopkgs
RUN go get -u github.com/sqs/goreturns
RUN go get -u github.com/mdempsky/gocode
RUN go get -u github.com/rogpeppe/godef
RUN go get -u github.com/go-delve/delve/cmd/dlv
RUN go get -u github.com/ramya-rao-a/go-outline

EXPOSE 1323