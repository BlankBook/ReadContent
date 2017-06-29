FROM golang:onbuild

WORKDIR /go/src/github.com/blankbook/readcontent
COPY . .

RUN go-wrapper download
RUN go-wrapper install

CMD ["go-wrapper", "run"]
