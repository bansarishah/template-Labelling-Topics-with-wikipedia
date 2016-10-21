import wikipedia
import predictionio
import argparse
import csv

def get_wikiPages(client,filename):
	pagesFile = open(filename,"r")
	for pageName in pagesFile:
		try:
			page = wikipedia.page(pageName)
			import_events(client,page.title,page.content)
			print(page.title +':imported successfully')
		except Exception as e:
			print('exception:',e)
	
def import_events(client,title,content):
    client.create_event(
        event="traindata",
        entity_type="WikiPage",
        entity_id=title,
        properties= { "category" : title, "content": content }
    )


if __name__ == '__main__':
  parser = argparse.ArgumentParser(
    description="import wikipedia pages data")
  parser.add_argument('--access_key', default='access_key')
  parser.add_argument('--url', default="http://localhost:7070")
  parser.add_argument('--file', default="./data/train.txt")

  args = parser.parse_args()
  print(args)

  client = predictionio.EventClient(
    access_key=args.access_key,
    url=args.url,
    threads=5,
    qsize=0)
  get_wikiPages(client,args.file)
