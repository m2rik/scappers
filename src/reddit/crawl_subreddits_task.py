import praw
import helpers
import reddit_helpers
import aggregate_task

import luigi

class CrawlSubredditsTask(luigi.Task):

	is_complete = False
	subreddits_to_crawl = luigi.Parameter('subreddits_to_crawl')


	def requires(self):
		config = helpers.load_configuration(r'./configs/reddit')
		reddit = praw.Reddit(client_id = config['client_id'], client_secret = config['client_secret'], user_agent = config['user_agent'])

		for subreddit in self.subreddits_to_crawl.split('|'):
			reddit_subreddit = reddit.subreddit(subreddit)
			yield aggregate_task.AggregateTask(subreddit, reddit_helpers.get_start(reddit_subreddit))


	def run(self):
		self.is_complete = True


	def complete(self):
		return self.is_complete
    	



if __name__ == '__main__':
    luigi.run()
