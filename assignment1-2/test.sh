export GOPATH="$PWD"

if [ "$1" == "-c" ]; then
    ( find "." -name "*mrtmp*" -exec rm {} \; )
    ( find "." -name "*diff*" -exec rm {} \; )
else
    ( cd "$GOPATH/src";  go test -v -run Sequential mapreduce/... )
    ( cd "$GOPATH/src/main"; sh ./test-wc.sh )
fi

