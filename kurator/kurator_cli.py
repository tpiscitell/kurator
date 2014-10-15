#!/usr/bin/python

from kazoo.client import KazooClient
from kafka import KafkaClient, SimpleConsumer
import argparse, json, sys, pprint, os
from datetime import datetime

import kurator

class bcolors:
  HEADER = '\033[95m'
  OKBLUE = '\033[94m'
  OKGREEN = '\033[92m'
  WARNING = '\033[93m'
  FAIL = '\033[91m'
  ENDC = '\033[0m'

def make_parser():
  parser = argparse.ArgumentParser(description='Save kafka messages for later')
  
  # can specify one broker to get meta data from or use zookeeper
  server_group = parser.add_mutually_exclusive_group(required=True)
  server_group.add_argument('-z', '--zookeeper', help='comma separated list of zookeeper servers',
      metavar='SERVER1,SERVER2...', dest='zookeeper_servers')
  server_group.add_argument('-b', '--broker', help='comma separated list of kafka brokers',
      metavar='SERVER1,SERVER2...', dest='kafka_brokers')

  # can specifiy a specific topic to operate on or a regex to match against
  topic_group = parser.add_mutually_exclusive_group()
  topic_group.add_argument('-t', '--topic', metavar='TOPIC', dest='topic',
      help='topic to be saved for later')
  topic_group.add_argument('-r', '--regex', help='topic names matching this regex will be saved.',
      metavar='REGEX', dest='regex', default='.*')
  
  subparsers = parser.add_subparsers(
      title='Commands', dest='command', description='Select one of the following commands',
      help='Run: %s COMMAND --help for command specific help.' % sys.argv[0])
  
  #size
  parser_size = subparsers.add_parser('size', help='List size of topics')
  parser_size.set_defaults(func=cli_size)

  #list
  parser_list = subparsers.add_parser('list', help='List topics in cluster')
  parser_list.set_defaults(func=cli_list)

  #peek
  parser_peek = subparsers.add_parser('peek', help='View messages in topics')
  parser_peek.set_defaults(func=cli_peek)
  parser_peek.add_argument('--count', help='number of messages to read',
      metavar='#', dest='count', default=1, type=int)
  parser_peek.add_argument('--offset', help='where in the topic to read from 0=oldest, -1=newest',
      metavar='#', dest='offset', default='-1', type=int)
  parser_peek.add_argument('--pretty', help='pretty print message values if possible',
      dest='pretty', action='store_true', default=False)

  #preserve
  parser_preserve = subparsers.add_parser('preserve', help='Save topics locally')
  parser_preserve.set_defaults(func=cli_preserve)
  parser_preserve.add_argument('--outdir', help='where to save the topics to for later',
      metavar='DIR', dest='outdir', default=os.path.join(os.getcwd(), 'kurator_cli_%s' % datetime.utcnow().strftime('%Y_%m_%d')))

  return parser

def zk_broker_list(zk):
  return ','.join([ '%s:%s' % (b['host'], b['port']) for b in kurator.brokers_from_zk(zk) ])

def is_ascii(s):
  try:
    s.decode('ascii')
    return True
  except UnicodeDecodeError:
    return False

def is_utf8(s):
  try:
    s.decode('utf8')
    return True
  except UnicodeDecodeError:
      return False

def make_pretty(s):
  '''Try various common value types and return a prettier version of it.'''
  
  # json
  try:
    d = json.loads(s)
    return pprint.pformat(d)
  except ValueError:
    pass

  # hex
  if not is_ascii(s) and not is_utf8(s):
    hex_string =  ' '.join("{0:02x}".format(ord(c)) for c in s)
    return  '\n'.join([ hex_string[i:i+48] for i in range(0, len(hex_string), 48) ])

def color_kv(k,v):
  ret = bcolors.HEADER + str(k) + bcolors.ENDC + ': '
  ret += str(v)
  return ret

def color_dict(d):
  return '{' + ', '.join([ color_kv(k,d[k]) for k in d.keys() ]) + '}'

def cli_size(kafka_client, **kwargs):
  for topic, size, partitions in kurator.topic_size(kafka_client, **kwargs):
    if size == 0:
      color = bcolors.FAIL
    else:
      color = bcolors.OKGREEN

    print color + topic + bcolors.ENDC + ('(%s)' % size) + \
        ' = ' + color_dict(partitions)

def cli_list(kafka_client, **kwargs):
  for topic in kurator.topic_list(kafka_client, **kwargs):
    print topic

def cli_preserve(kafka_client, **kwargs):
  kurator.topic_preserve(kafka_client, **kwargs)

def cli_peek(kafka_client, **kwargs):
  
  pretty = kwargs['pretty'] if 'pretty' in kwargs else False

  for topic, res in kurator.topic_peek(kafka_client, **kwargs):
    if pretty:
      value =  '\n' + make_pretty(res.message.value)
    else:
      value = repr(res.message.value)
    
    print bcolors.OKGREEN + topic + bcolors.ENDC + '[' + bcolors.HEADER + \
        str(res.offset) + bcolors.ENDC + '] = ' + value

def main():
  parser = make_parser()
  args = parser.parse_args()
  
  if args.zookeeper_servers:
    zk = KazooClient(args.zookeeper_servers)
    zk.start()
    brokers =  zk_broker_list(zk)
  else:
    brokers = args.kafka_brokers

  kafka_client = KafkaClient(brokers)
  argsdict = args.__dict__
  args.func(kafka_client, **argsdict)

if __name__ == '__main__':
  main()
