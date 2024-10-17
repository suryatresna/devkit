# Devkit Toolkit
Mini toolkit for any development tools

# How to use

Install the package with the following command:
```bash
go install github.com/suryatresna/devkit@latest
```

# Command List

## Kafka
```bash
devkit kafka produce --brokers 127.0.0.1:19092 --topic stock.costing  --jsonfile tmp/testjson.json
```

## Gocraft
```bash
devkit gocraft worker --redis 127.0.0.1:6379 --ns fooworker  --job foojob  --json '{"myid":1234}'
```

# Feature Incoming
- [ ] Client consuming
- [ ] Create Topic
- [ ] Delete Topic