
rm -rf /tmp/gopath
mkdir /tmp/gopath
export GOPATH=/tmp/gopath
mkdir -p /tmp/gopath/src/github.com/fabregas/fabnet_go
rsync -az ./ /tmp/gopath/src/github.com/fabregas/fabnet_go/
cd /tmp/gopath/src/github.com/fabregas/fabnet_go
go get -t -v ./...
go install ./...

bash run_tests.sh
