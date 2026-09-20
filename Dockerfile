FROM docker.m.daocloud.io/library/golang:1.24
WORKDIR /app
COPY . .
CMD ["go", "test", "./..."]
