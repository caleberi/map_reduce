build_wc:
	go build -buildmode=plugin -trimpath -o build/wc.so plugins/wc.go
build_indexer:
	go build -buildmode=plugin -trimpath -o build/indexer.so plugins/indexer.go
build_fc:
	go build -buildmode=plugin -trimpath -o build/fc.so plugins/fc.go
build_main:
	 go build -trimpath -o main main.go
.PHONY:
	build_wc build_main build_indexer build_fc