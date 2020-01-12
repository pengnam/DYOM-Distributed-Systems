cd ../mrapps
go build -buildmode=plugin indexer.go
cd ../main
rm mr-out*
rm test-[0-9]*
go run mrworker.go ../mrapps/indexer.so
