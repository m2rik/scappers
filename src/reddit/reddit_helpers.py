import pandas
import helpers

def get_author(author):
    if author == None:
        return author

    return author.name
	
	
def get_start(subreddit):
    return subreddit.created_utc


def get_submission(submission):
    return {
        'approved_at_utc'         : getattr(submission, 'approved_at_utc', None),
        'approved_by'             : getattr(submission, 'approved_by', None),
        'archived'                : getattr(submission, 'archived', None),
        'author_flair_css_class'  : getattr(submission, 'author_flair_css_class', None),
        'author_flair_text'       : getattr(submission, 'author_flair_text', None),
        'banned_at_utc'           : getattr(submission, 'banned_at_utc', None),
        'banned_by'               : getattr(submission, 'banned_by', None),
        'brand_safe'              : getattr(submission, 'brand_safe', None),
        'can_gild'                : getattr(submission, 'can_gild', None),
        'can_mod_post'            : getattr(submission, 'can_mod_post', None),
        'clicked'                 : getattr(submission, 'clicked', None),
        'comment_limit'           : getattr(submission, 'comment_limit', None),
        'comment_sort'            : getattr(submission, 'comment_sort', None),
        'contest_mode'            : getattr(submission, 'contest_mode', None),
        'created'                 : getattr(submission, 'created', None),
        'created_utc'             : getattr(submission, 'created_utc', None),
        'distinguished'           : getattr(submission, 'distinguished', None),
        'domain'                  : getattr(submission, 'domain', None),
        'downs'                   : getattr(submission, 'downs', None),
        'edited'                  : getattr(submission, 'edited', None),
        'gilded'                  : getattr(submission, 'gilded', None),
        'hidden'                  : getattr(submission, 'hidden', None),
        'hide_score'              : getattr(submission, 'hide_score', None),
        'id'                      : getattr(submission, 'id', None),
        'is_crosspostable'        : getattr(submission, 'is_crosspostable', None),
        'is_reddit_media_domain'  : getattr(submission, 'is_reddit_media_domain', None),
        'is_self'                 : getattr(submission, 'is_self', None),
        'is_video'                : getattr(submission, 'is_video', None),
        'likes'                   : getattr(submission, 'likes', None),
        'link_flair_css_class'    : getattr(submission, 'link_flair_css_class', None),
        'link_flair_text'         : getattr(submission, 'link_flair_text', None),
        'locked'                  : getattr(submission, 'locked', None),
        'media_embed'             : getattr(submission, 'media_embed', None),
        'mod_note'                : getattr(submission, 'mod_note', None),
        'mod_reason_by'           : getattr(submission, 'mod_reason_by', None),
        'mod_reason_title'        : getattr(submission, 'mod_reason_title', None),
        'mod_reports'             : getattr(submission, 'mod_reports', None),
        'name'                    : getattr(submission, 'name', None),
        'num_comments'            : getattr(submission, 'num_comments', None),
        'num_crossposts'          : getattr(submission, 'num_crossposts', None),
        'num_reports'             : getattr(submission, 'num_reports', None),
        'over_18'                 : getattr(submission, 'over_18', None),
        'parent_whitelist_status' : getattr(submission, 'parent_whitelist_status', None),
        'permalink'               : getattr(submission, 'permalink', ''),
        'pinned'                  : getattr(submission, 'pinned', None),
        'post_hint'               : getattr(submission, 'post_hint', None),
        'quarantine'              : getattr(submission, 'quarantine', None),
        'removal_reason'          : getattr(submission, 'removal_reason', None),
        'report_reasons'          : getattr(submission, 'report_reasons', None),
        'saved'                   : getattr(submission, 'saved', None),
        'score'                   : getattr(submission, 'score', None),
        'secure_media'            : getattr(submission, 'secure_media', None),
        'selftext'                : helpers.remove_new_lines(getattr(submission, 'selftext', '')),
        'spoiler'                 : getattr(submission, 'spoiler', None),
        'stickied'                : getattr(submission, 'stickied', None),
        'subreddit_id'            : getattr(submission, 'subreddit_id', None),
        'suggested_sort'          : getattr(submission, 'suggested_sort', None),
        'title'                   : helpers.remove_new_lines(getattr(submission, 'title', '')),
        'ups'                     : getattr(submission, 'ups', None),
        'url'                     : getattr(submission, 'url', ''),
        'user_reports'            : getattr(submission, 'user_reports', None),
        'view_count'              : getattr(submission, 'view_count', None),
        'visited'                 : getattr(submission, 'visited', None),
        'whitelist_status'        : getattr(submission, 'whitelist_status', None),
        
        # custom fields ...
        'author'                  : get_author(submission.author)
    }

	
def get_comment(comment, submissionId):
    return {
        'approved_at_utc'        : getattr(comment, 'approved_at_utc', None),
        'approved_by'            : getattr(comment, 'approved_by', None),
        'archived'               : getattr(comment, 'archived', None),
        'author_flair_css_class' : getattr(comment, 'author_flair_css_class', None),
        'author_flair_text'      : getattr(comment, 'author_flair_text', None),
        'banned_at_utc'          : getattr(comment, 'banned_at_utc', None),
        'banned_by'              : getattr(comment, 'banned_by', None),
        'body'                   : helpers.remove_new_lines(getattr(comment, 'body', '')),
        'can_gild'               : getattr(comment, 'can_gild', None),
        'can_mod_post'           : getattr(comment, 'can_mod_post', None),
        'collapsed'              : getattr(comment, 'collapsed', None),
        'collapsed_reason'       : getattr(comment, 'collapsed_reason', None),
        'controversiality'       : getattr(comment, 'controversiality', None),
        'created'                : getattr(comment, 'created', None),
        'created_utc'            : getattr(comment, 'created_utc', None),
        'depth'                  : getattr(comment, 'depth', None),
        'distinguished'          : getattr(comment, 'distinguished', None),
        'downs'                  : getattr(comment, 'downs', None),
        'edited'                 : getattr(comment, 'edited', None),
        'gilded'                 : getattr(comment, 'gilded', None),
        'id'                     : getattr(comment, 'id', None),
        'is_submitter'           : getattr(comment, 'is_submitter', None),
        'likes'                  : getattr(comment, 'likes', None),
        'link_id'                : getattr(comment, 'link_id', None),
        'mod_note'               : getattr(comment, 'mod_note', None),
        'mod_reason_by'          : getattr(comment, 'mod_reason_by', None),
        'mod_reason_title'       : getattr(comment, 'mod_reason_title', None),
        'mod_reports'            : getattr(comment, 'mod_reports', None),
        'name'                   : getattr(comment, 'name', None),
        'num_reports'            : getattr(comment, 'num_reports', None),
        'parent_id'              : getattr(comment, 'parent_id', None),
        'permalink'              : getattr(comment, 'permalink', ''),
        'removal_reason'         : getattr(comment, 'removal_reason', None),
        'report_reasons'         : getattr(comment, 'report_reasons', None),
        'saved'                  : getattr(comment, 'saved', None),
        'score'                  : getattr(comment, 'score', None),
        'score_hidden'           : getattr(comment, 'score_hidden', None),
        'stickied'               : getattr(comment, 'stickied', None),
        'subreddit_id'           : getattr(comment, 'subreddit_id', None),
        'ups'                    : getattr(comment, 'ups', None),
        'user_reports'           : getattr(comment, 'user_reports', None),
        
        # custom fields ...
        'submissionId'           : submissionId,
        'author'                 : get_author(comment.author)
    }

	
def write_to_csv(output_directory, comments, start_at, end_at):		
    comments_csv_path = r'{}/comments_for_posts_{}_{}.csv'.format(output_directory, start_at, end_at)
    if len(comments) > 0:
        df_comments = pandas.DataFrame(comments)
        df_comments.index.names = ['index']
        df_comments.to_csv(comments_csv_path, header=True)
    else:
        with open(comments_csv_path, 'w') as comments_csv_file:
            pass