# pgoxy
PostgreSQL proxy

## How to run

```sh 
$ go build cmd/service/main.go -o pgoxy
$ ./pgoxy -listen=192.168.14.7:6432 -upstream=your_ip_postgres:5432
```
Connect to 192.168.14.7:6432
