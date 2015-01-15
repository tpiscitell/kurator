# Kurator

This tool helps Kafka cluster administrators do useful things with their clusters.

## Inspiration

The inspiration for this tool is Elasticsearch's [Curator](https://github.com/elasticsearch/curator), hence the name. I wanted a tool as useful as curator but for Kafka clusters intead of Elasticsearch clusters.

## Installation

Get a local copy and install using pip

    pip install -e ./kurator

## API Usage

Much of the API has doc strings. Until I can find time to put together proper documentation `help(kurator.<function>)` should suffice for now.

## CLI Usage

Kurator can pull broker and topic information from either a seed list of Brokers or a Zookeeper cluster (`--broker` or `--zookeeper`). It can interact with a single topic, or a list of topics matching a given regular expression (`--topic` or `--regex`). If you do not supply either a topic or a regular expression, all found topics will be used.

See `kurator --help` for usage specifics and `kurator COMMAND --help` for usage of specific commands.

### CLI Commands

* list - List topics in the cluster
* size - List topics and the number of messages in them
* save - Save a local copy of messages in topics
* load - Loads a saved local copy of topic messages into the cluster

