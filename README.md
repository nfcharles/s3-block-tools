# Blocks

Tools for identifying block ids and invoking lambda functions.


Below represents an example block filesystem structure in s3.  The following examples will
assume this layout.
```bash
  prompt> aws s3 ls --recursive s3://test//foo/
  /foo/a=1
  /foo/a=1/b=1
  /foo/a=1/b=1/000000_0
  /foo/a=1/b=2
  /foo/a=1/b=2/000000_0
  /foo/a=1/b=3
  /foo/a=1/b=3/000000_0
  /foo/a=2
  /foo/a=2/b=1
  /foo/a=2/b=1/000000_0
  /foo/a=2/b=2
  /foo/a=2/b=2/000000_0
  /foo/a=2/b=3
  /foo/a=2/b=3/000000_0
  /foo/a=3
  /foo/a=3/b=1
  /foo/a=3/b=1/000000_0
  /foo/a=3/b=2
  /foo/a=3/b=2/000000_0
  /foo/a=3/b=3
  /foo/a=3/b=3/000000_0
  block_-1381873372567827756
  block_-9167327871567959482
  block_1094235986601944229
  block_7692193377734623887
  .
  .
  .
  .

```

## CLI

```bash
BLOCKS_BUILD=target/uberjar/blocks-0.1.0-SNAPSHOT-standalone.jar
alias blocks='java -jar $BLOCKS_BUILD'
```

```bash
prompt> blocks -h
Utility for identifying block files and invoking lambda functions with s3 events.

Usage: blocks [options] find|lambda

Options:
  -h, --help
```

### Find

```bash
prompt> blocks find -h
Identifies block files from BUCKET filtered by PREFIXES.  Block files from each PREFIX
are written to FILES respectively.  S3 block files are identified by EXPRESSION (regexp).

Usage: blocks [options] find BUCKET PREFIXES FILES EXPRESSION

Options:
  -t, --threads THREAD  4  Thread count
  -h, --help
```

#### Examples

##### Find all blocks for single prefix
```bash
PREFIXES=/foo/a=1
FILES=blocks/a
EXPRESSION=foo/a=\d*/b=\d*/[\d]{6}_\d
prompt> blocks find -t 10 test $PREFIXES $FILES $EXPRESSION
```

##### Find all blocks for multiple prefixes
```bash
PREFIXES=/foo/a=1,/foo/a=2
FILES=blocks/a_1,blocks/a_2
EXPRESSION=foo/a=\d*/b=\d*/[\d]{6}_\d
prompt> blocks find -t 10 test $PREFIXES $FILES $EXPRESSION
```


### Lambda
```bash
prompt> blocks lambda -h
Submits events for lambda execution.  At most CONCURRENCY events are executed concurrently.

Usage: blocks [options] lambda PATH FUNCTION

Options:
  -c, --concurrency CONCURRENCY                  10     Max concurrent lambda invocations
  -i, --input-type INPUT_TYPE ("block"|"event")  block  Input file type
  -h, --help
```

#### Examples

##### Invoke lambda with s3 event source derived from block file 
```bash
prompt> blocks -c 8 blocks/a_1.json my-lambda-function
```

File contents
```bash
[
    {
        "bkt": "test",
        "id": "block_3958294509727798951",
        "len": 2220
    },
    {
        "bkt": "test",
        "id": "block_6550756005239491563",
        "len": 1291377
    }
]
```

##### Invoke lambda with s3 event source
```bash
prompt> blocks -c 8 --event-type event source-events.json my-lambda-function
```

##### Retrying (partial) failures
All events from failed lambda invocations are dumped into an error file, [md5-signature].json

Rerun as follows
```bash
prompt> blocks -c 8 --input-type event e8370be5511dd42c04777daeff07476a.json my-lambda-function
```


## License

Copyright © 2017 Navil Charles

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
