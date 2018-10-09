import os
import json
import praw

import reddit_helpers

import luigi

class RedditScrapperTask(luigi.Task):
    subreddit_name = luigi.Parameter('subreddit_name')

    start_at = luigi.IntParameter('start_at')
    end_at = luigi.IntParameter('end_at')
	
    timeout_after_comment_in_secs = 250

    completed = False

    def output(self):
        output_directory = './data/reddit/{}/downloaded'.format(self.subreddit_name)
        return [
            luigi.LocalTarget('{}/posts_{}_{}.csv'.format(output_directory, self.start_at, self.end_at)),
            luigi.LocalTarget('{}/comments_{}_{}.csv'.format(output_directory, self.start_at, self.end_at))
        ]


    def run(self):
        config = reddit_helpers.load_configuration(r'./configs/reddit')
        reddit = praw.Reddit(client_id = config['client_id'], client_secret = config['client_secret'], user_agent = config['user_agent'], username=config['username'])
        
        subreddit = reddit.subreddit(self.subreddit_name)

        posts = []
        comments = []
		#.submissions(start = self.start_at, end = self.end_at)
        for submission in subreddit.search('kim'):

            s = reddit_helpers.get_submission(submission, 1)
            posts.append(s)

            submission.comments.replace_more(limit=None)
            for comment in submission.comments.list():

                c = reddit_helpers.get_comment(comment, s['id'])
                comments.append(c)
		
        if not os.path.exists(r'./data/reddit'):
            os.mkdir(r'./data/reddit')
        if not os.path.exists(r'./data/reddit/{}'.format(self.subreddit_name)):
            os.mkdir(r'./data/reddit/{}'.format(self.subreddit_name))
        if not os.path.exists(r'./data/reddit/{}/downloaded'.format(self.subreddit_name)):
            os.mkdir(r'./data/reddit/{}/downloaded'.format(self.subreddit_name))
        if not os.path.exists(r'./data/reddit/{}/by_day'.format(self.subreddit_name)):
            os.mkdir(r'./data/reddit/{}/by_day'.format(self.subreddit_name))
        if not os.path.exists(r'./data/reddit/{}/by_day/comments'.format(self.subreddit_name)):
            os.mkdir(r'./data/reddit/{}/by_day/comments'.format(self.subreddit_name))
        if not os.path.exists(r'./data/reddit/{}/by_day/posts'.format(self.subreddit_name)):
            os.mkdir(r'./data/reddit/{}/by_day/posts'.format(self.subreddit_name))

        reddit_helpers.write_to_csv(r'./data/reddit/{}/downloaded'.format(self.subreddit_name), posts, comments, self.start_at, self.end_at)
        self.completed = True


    def complete(self):
        return self.completed



if __name__ == '__main__':
    luigi.run()
