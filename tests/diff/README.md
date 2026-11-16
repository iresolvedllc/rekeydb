# Differential harness

`diff.py` sends the same command sequence to two live RESP servers and compares
the raw replies. It is a fast complement to the Tcl suite, not a replacement.

```sh
# Start KeyDB and rekeydb on separate ports first.
./tests/diff/diff.py \
    --a-port 6379 --b-port 6380 \
    --trace tests/diff/traces/core.txt

./tests/diff/diff.py --a-port 6379 --b-port 6380 SET probe value
```

Use the same database selection and authentication on both sides. Replies that
depend on wall-clock time (`TTL`, `TIME`), random selection, or iteration order
should be excluded or normalized before they are added to a trace.

The next version will record and replay RESP streams and compare `DEBUG DIGEST`
state after every batch.
