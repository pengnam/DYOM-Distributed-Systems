cd ../mrapps
go build -buildmode=plugin wc.go
cd ../main
rm mr-out*
rm test-[0-9]*
go run mrworker.go ../mrapps/wc.so
