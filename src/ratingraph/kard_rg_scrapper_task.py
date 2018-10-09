import os
import pandas
import requests
from bs4 import BeautifulSoup

import luigi

class KardRatinGraphScrapperTask(luigi.Task):


	def output(self):
		return luigi.LocalTarget(r'./data/kard_rg/ratings.csv')


	def run(self):
	
		response = requests.get('https://www.ratingraph.com/serie/keeping_up_with_the_kardashians-45233/')
		assert response.status_code == 200

		soup = BeautifulSoup(response.content, 'html.parser')

		ratings_collection = []
		can_start_collecting = False
		for line in [ line for line in soup.find_all('script')[0].text.split('\n') if len(line) > 0 ]:
			if not can_start_collecting:
				can_start_collecting = 'tableEpisodes' in line
				if can_start_collecting:
					line = line.replace('var tableEpisodes =', '')
			
			if can_start_collecting:
				ratings_collection.append(line)
				
		## remove the last ';'
		ratings_collection[-2] = ratings_collection[-2].replace(';', '')
		kard_ratings = eval(''.join(ratings_collection))

		for rating in kard_ratings:
			
			ms_ = [
				('&#039;', "'"),
				('&quot;', '"')
			]
			
			for key in [ 'name', 'description' ]:
				for m in ms_:
					rating[key] = rating[key].replace(m[0], m[1])

		with self.output().open('w') as output_file:
			pandas.DataFrame(kard_ratings).to_csv(output_file)




if __name__ == '__main__':
    luigi.run()