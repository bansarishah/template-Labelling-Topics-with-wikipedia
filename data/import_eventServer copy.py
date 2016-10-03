"""
Import sample data for sentiment analysis engine
"""

import predictionio
import argparse

def import_events(client, file):
  f = open(file, 'r')
  count = 0
  print("Importing data...")
  for line in f:
    data = line.rstrip('\r\n').strip().rsplit(',')
    print(data,len(data))
    client.create_event(
      event="train",
      entity_type="phrase",
      entity_id=count,
      properties= { "phrase" : data[3], "sentiment": data[0]) }
    )
    count += 1
  f.close()
  print("%s events are imported." % count)

if __name__ == '__main__':
  parser = argparse.ArgumentParser(
    description="Import rotten tomatoes data for sentiment analysis")
  parser.add_argument('--access_key', default='invald_access_key')
  parser.add_argument('--url', default="http://localhost:7070")
  parser.add_argument('--file', default="./data/train.txt")

  args = parser.parse_args()
  print(args)

  client = predictionio.EventClient(
    access_key=args.access_key,
    url=args.url,
    threads=5,
    qsize=500)
  import_events(client, args.file)
