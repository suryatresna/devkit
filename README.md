# Devkit Toolkit
Mini toolkit for any development tools

# How to use

Install the package with the following command:
```bash
go install github.com/suryatresna/devkit@latest
```

# Command List

## Kafka
*Produce Message*
```bash
devkit kafka produce --brokers 127.0.0.1:19092 --topic topic.foo.bar  --jsonfile tmp/testjson.json
```

*Manual Commit*
```bash
devkit kafka commit --brokers localhost:9092 --group my-group --topic my-topic --poll 1
```

*Reset Offset*
```bash
devkit kafka offset --brokers localhost:9092 --group my-group --topic my-topic --offset 0 --datetime "3/19/2025, 12:06:37"
```

* offset 0 will use datetime as offset, if datetime not set, that will use default date `time.Now()`
* offset 1 will use beginning of offset
* offset -1 will use end of offset



## Gocraft
```bash
devkit gocraft worker --redis 127.0.0.1:6379 --ns fooworker  --job foojob  --json '{"myid":1234}'
```

# Feature Incoming
- [ ] Client consuming
- [ ] Create Topic
- [ ] Delete Topic
