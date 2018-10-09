import os
import time
import praw
import luigi

import helpers
import reddit_helpers
import pushshift_helpers

class ScrapperTask(luigi.Task):

	subreddit = luigi.Parameter('subreddit')
	start_at = luigi.IntParameter('start-at')
	end_at = luigi.IntParameter('end-at')


	def output(self):
		return [
			luigi.LocalTarget(r'./data/reddit/{}/downloaded/posts_{}_{}.csv'.format(self.subreddit, self.start_at, self.end_at)),
			luigi.LocalTarget(r'./data/reddit/{}/downloaded/comments_for_posts_{}_{}.csv'.format(self.subreddit, self.start_at, self.end_at))
		]


	def run(self):
		TIMEOUT_AFTER_COMMENT_IN_SECS = .250
		config = helpers.load_configuration(r'./configs/reddit')
		reddit = praw.Reddit(client_id = config['client_id'], client_secret = config['client_secret'], user_agent = config['user_agent'])
		
		comments = []
		posts = []

		for post in pushshift_helpers.pull_posts_for(self.subreddit, self.start_at, self.end_at):
			submission = reddit.submission(id=post['id'])
			
			posts.append(
				reddit_helpers.get_submission(submission))

			submission.comments.replace_more(limit=None)
			for comment in submission.comments.list():
				comments.append(
					reddit_helpers.get_comment(comment, post['id']))
				if TIMEOUT_AFTER_COMMENT_IN_SECS > 0:
					time.sleep(TIMEOUT_AFTER_COMMENT_IN_SECS)
		
		output_directory = r'./data/reddit/{}/downloaded'.format(self.subreddit)
		helpers.verify_directories([ output_directory ])

		pushshift_helpers.write_to_csv(output_directory, posts, self.start_at, self.end_at)
		reddit_helpers.write_to_csv(output_directory, comments, self.start_at, self.end_at)




if __name__ == '__main__':
	luigi.run()
