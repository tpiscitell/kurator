#!/usr/bin/python

from kafka import SimpleConsumer
from kafka.common import OffsetRequest, OffsetOutOfRangeError, FetchRequest
import sys, json, re, os
from datetime import datetime
from getpass import getuser
from base64 import b64encode, b64decode

def topics_from_zk(zk):
  '''
  Returns a list of topics from zookeeper

  :arg zk: Zookeeper client to use

  :rtype: generator of strings
  '''
  # kafka lib doesnt like unicode..
  for t in zk.get_children('/brokers/topics'):
    yield t.encode('ascii')

def zk_fetch(zk, path):
  '''
  Wrapper for zk.get to only return the values
  
  :arg zk: Zookeeper client to use
  :arg path: Zookeeper path to get

  :rtype: dict if path contains json. Otherwise, a string
  '''
  vals, stats = zk.get(path)
  try:
    data = json.loads(vals)
  except ValueError:
    data = vals
  return data

def brokers_from_zk(zk):
  '''
  Return a list of Kafka Brokers from Zookeeper

  :arg zk: Zookeeper client to use

  :rtype: generator of dicts
  '''
  ids = zk.get_children('/brokers/ids/')
  for broker_id in ids:
    yield zk_fetch( zk, '/brokers/ids/%s/' % broker_id)

def filter_topics_by_regex(kafka_client, **kwargs):
  '''
  Return a filtered list of kafka topics

  :arg kafka_client: The Kafka Client
  :arg regex: The regular expression to match topic names against
  :arg use_zk: Use zookeeper instead of the Kafka Client get list topics
  :arg zk: The zookeeper client to fetch topics from

  :rtype: generator of strings
  '''

  regex = kwargs['regex'] if 'regex' in kwargs else '.*'
  use_zk = kwargs['use_zk'] if 'use_zk' in kwargs else False
  zk = kwargs['zk'] if 'zk' in kwargs else None

  topic_regex = re.compile(regex)

  if use_zk is not None and zk is not None:
    topics = [ t.encode('ascii') for t in topics_from_zk(zk) ]
  else:
    topics = kafka_client.topic_partitions.keys()

  for topic in topics:
    if topic_regex.match(topic):
      yield topic

def unix_timestamp(dt):
    return (dt - datetime(1970, 1, 1)).total_seconds()

#kurator size
def topic_size(kafka_client, **kwargs):
  '''
  Print number of msgs in each topic

  :arg kafka_client: Kafka client to use
  :arg topic: optional single topic to use instead of a list
  :arg regex: optional regex to filter topics by
  :arg use_zk: Use zookeeper instead of the Kafka Client get list topics
  :arg zk: The zookeeper client to fetch topics from

  :rtype: generator of tuples (topic, total_messages, {partition: messages})
  '''
  
  topic = kwargs['topic'] if 'topic' in kwargs else None
  
  if topic is not None:
    topics = [ topic ]
  else:
    topics = filter_topics_by_regex(kafka_client, **kwargs)

  for t in topics:
    c = SimpleConsumer(kafka_client, 'kurator9', t)
    
    offset_counts = {}
    total = 0
    for partition in c.offsets.keys():
      old = c.client.send_offset_request([OffsetRequest(c.topic, partition, -2, 1)])
      new = c.client.send_offset_request([OffsetRequest(c.topic, partition, -1, 1)])
      offset_counts[partition] = new[0].offsets[0] - old[0].offsets[0]
      total += offset_counts[partition]

    yield (t, total, offset_counts)

# kurator list
def topic_list(kafka_client, **kwargs):
  '''
  Print a list of topics in the Kafka cluster

  :arg kafka_client: Kafka client to use
  :arg regex: optional regex to filter list with
  :arg use_zk: Use zookeeper instead of the Kafka Client get list topics
  :arg zk: The zookeeper client to fetch topics from

  :rtype generator of strings
  '''

  for t in filter_topics_by_regex(kafka_client, **kwargs):
    yield t

# kurator peek
def topic_peek(kafka_client, **kwargs):
  '''
  Peek at messages in topics

  :arg kafka_client: Kafka client to use
  :arg count: number of messages to read from topics
  :arg offset: where in the topics to read from (-1=newest, 0=oldest)
  :arg topic: option single topic to use instead of a list
  :arg regex: optional regex to filter topic list with
  :arg use_zk: Use zookeeper instead of the Kafka Client get list topics
  :arg zk: The zookeeper client to fetch topics from

  :rtype: generator of tuples (topic, OffsetAndMessage object)
  '''

  topic = kwargs['topic'] if 'topic' in kwargs else None
  count = kwargs['count'] if 'count' in kwargs else 1
  offset = kwargs['offset'] if 'offset' in kwargs else -1

  if topic is not None:
    topics = [ topic ]
  else:
    topics = filter_topics_by_regex(kafka_client, **kwargs)

  for t in topics:
    c = SimpleConsumer(kafka_client, 'kurator', t, auto_commit=False)
    if offset == -1:
      c.seek(-1, 2)
      pass
    else:
      c.seek(offset, 0)

    # If the newest offset is > 0, but all the data has aged off in kafka,
    # then we will try to get a message that does not exist and 
    # a kafka.common.OffsetOutOfRangeError is raised by python-kafka
    try: 
      messages = c.get_messages(count)
    except OffsetOutOfRangeError:
      messages = []
    for om in messages:
      yield (t, om)

# kurator prserve
def topic_preserve(kafka_client, **kwargs):
  '''
  Save a local copy of topics in the Kafka cluster to disk

  :args kafka_client: Kafka client to use
  :args outdir: where to save the local copies to
  :args topic: optional single topic to use instead of a list
  :args regex: optional regex to filter topics by
  :arg use_zk: Use zookeeper instead of the Kafka Client get list topics
  :arg zk: The zookeeper client to fetch topics from
 '''

  topic = kwargs['topic'] if 'topic' in kwargs else None

  default_outdir = os.path.join(os.getcwd(), 'kurator_%s' % datetime.now().strftime('%y_%m_%d'))
  outdir = kwargs['outdir'] if 'outdir' in kwargs else default_outdir

  if topic is not None:
    topics = [ topic ]
  else:
    topics = filter_topics_by_regex(kafka_client, **kwargs)

  metadata = { 
      'start_time': unix_timestamp(datetime.utcnow()),
      'user' : getuser(),
      'topics' : topics,
      'outdir':  outdir
      }

  if not os.path.exists(outdir):
    os.mkdir(outdir)
  
  for t in topics:
    c = SimpleConsumer(kafka_client, 'kurator9', t)
    with open(os.path.join(outdir, ('%s.json' % t)), 'w') as f:
      offset_counts = {}
      total = 0
      for partition in c.offsets.keys():
        old = c.client.send_offset_request([OffsetRequest(c.topic, partition, -2, 1)])
        new = c.client.send_offset_request([OffsetRequest(c.topic, partition, -1, 1)])

        # TODO switch back to using the whole partition instead of just 5
        # reqs = [ FetchRequest(c.topic, partition, x, c.buffer_size) for x in range(old[0].offsets[0], new[0].offsets[0]) ]
        reqs = [ FetchRequest(c.topic, partition, old[0].offsets[0] + x, c.buffer_size) for x in range(5) ]
        for res in c.client.send_fetch_request(reqs):
          for om in res.messages:
            record = {
                'topic': res.topic,
                'partition': res.partition,
                'message': { 
                  'key': om.message.key,
                  'value': b64encode(om.message.value)
                  }
                }
            f.write(json.dumps(record))

  metadata['stop_time'] = unix_timestamp(datetime.utcnow())
  print metadata
