# Node Definitions

## Transform Node

## Source Node

## Sink Node

# Supported Sources

### Batch

- MongoDB
- File
  - .csv
  - xslx
  - .txt
- ElasticSearch

### Stream

- Kafka

# What is exactly termed as a Pipeline?

A pipeline is set of **independent** jobs both stream and batch that is scheduled or executed by a user

# What is a job?

A job is a single task that consists of populating data from one or multiple sources and applying one or more transformations and pushing the data into a sink
