cd ../mrapps
go build -buildmode=plugin mtiming.go
cd ../main
rm mr-out*
rm test-[0-9]*
go run mrworker.go ../mrapps/mtiming.so &
go run mrworker.go ../mrapps/mtiming.so
