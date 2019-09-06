export GOPATH="$PWD"
echo $GOPATH

if [ "$1" == "-c" ]; then
    ( find "." -name "*mrtmp*" -exec rm {} \; )
    ( find "." -name "*diff*" -exec rm {} \; )
    ( find "." -name "*mrinput*" -exec rm {} \; )
else
    ( cd "$GOPATH/src"; go test -v -run TestBasic mapreduce/... )
    ( cd "$GOPATH/src"; go test -v -run Failure mapreduce/... )
    ( cd "$GOPATH/src/main"; sh ./test-ii.sh )
fi

