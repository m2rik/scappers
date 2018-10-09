# scappers

odds_shark:
 - task to download and split
    - python .\src\odds_shark\odds_aggregate_task.py --local-scheduler OddsSharkAggregateTask

ratingraph:
 - task to download
    - python .\src\ratingraph\kard_rg_scrapper_task.py --local-scheduler KardRatinGraphScrapperTasks

reddit:
 - task to download
    - python .\src\reddit\reddit_aggregate_task.py --local-scheduler RedditAggregateTask --subreddit KUWTK --start 1538956800