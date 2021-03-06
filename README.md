# Kurator

This tool helps Kafka cluster administrators do useful things with their clusters.

## Inspiration

The inspiration for this tool is Elasticsearch's [Curator](https://github.com/elasticsearch/curator), hence the name. I wanted a tool as useful as curator but for Kafka clusters intead of Elasticsearch clusters.

## Installation

This has not been added to PyPi yet so to install, clone the repo and install using pip:

    git clone https://github.com/tpiscitell/kurator.git kurator
    pip install ./kurator

## API Usage

Much of the API has doc strings. Until I can find time to put together proper documentation `help(kurator.<function>)` should suffice for now.

## CLI Usage

Kurator can pull broker and topic information from either a seed list of Brokers or a Zookeeper cluster (`--broker` or `--zookeeper`). It can interact with a single topic, or a list of topics matching a given regular expression (`--topic` or `--regex`). If you do not supply either a topic or a regular expression, all found topics will be used.

See `kurator --help` for usage specifics and `kurator COMMAND --help` for usage of specific commands.

### CLI Commands

* list - List topics in the cluster
* size - List size of topics in the clsuter
* peek - View messages in a topic
* preserve - Dump contents of a Kafka topic to local file. Messages are stored in JSON format with base64 encoded values.
