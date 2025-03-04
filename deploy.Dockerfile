FROM golang:1.15

EXPOSE 80

WORKDIR /app
COPY . /app

RUN go build -o arques_order .