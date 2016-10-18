"""
Import sample data for sentiment analysis engine
"""

import predictionio
import argparse
import csv

def import_events(client, file):
  f = open(file, 'r')
  reader = csv.reader(f)
  print("Importing data...")
  rownum = 0
  for row in reader:
    if rownum == 0:
        header = row
        print(header[1],header[2])
    else:
        print(row)
        client.create_event(
           event="train",
           entity_type="wiki_page",
           entity_id=rownum,
           properties= { "category" : row[1], "content": row[2] }
        )
    rownum += 1
  f.close()
  print("%s events are imported." % rownum)

if __name__ == '__main__':
  parser = argparse.ArgumentParser(
    description="Import wikipedia dataset for labelling topics")
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
