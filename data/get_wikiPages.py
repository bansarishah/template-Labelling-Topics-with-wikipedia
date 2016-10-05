import wikipedia
import pandas

def get_wikiPages(filename):
	pagesFile = open(filename,"r")
	df = pandas.DataFrame(columns=['title','content'])
	titles = list()
	content = list()
	for pageName in pagesFile:
		try:
			page = wikipedia.page(pageName)
			titles.append(page.title)
			content.append(page.content)
		except:
			continue
	df['title'] = titles
	df['content'] = content	
	return df

df = get_wikiPages('list_of_wikipedia_pages_title.txt')		

df.to_csv('sample_wiki_pages_data.csv','a',header=True, cols=["title","content"],encoding='utf-8')




