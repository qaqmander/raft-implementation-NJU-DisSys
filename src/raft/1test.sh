FILENAME="/Users/qaqmander/Desktop/raft-output/output/output$2.txt"

echo start, $FILENAME

# go test -run $1 -race | tee $FILENAME
go test -run $1 -race >$FILENAME

echo end, $FILENAME
tail $FILENAME
