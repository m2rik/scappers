import luigi

import json
import datetime
import odds_helpers
import odds_scrapper_task

class OddsSharkSplitterTask(luigi.Task):
    sync_id = str(datetime.datetime.now())


    def requires(self):
        return odds_scrapper_task.OddsSharkScrapperTask(self.sync_id)


    def output(self):
        return [ ]


    def run(self):
        input_path = self.input()
        with input_path.open() as infile:
            sync = json.loads(infile.read())

        parameter = odds_helpers.convert_date_to_filename(self.sync_id)

        books_target = luigi.LocalTarget('./data/odds_shark/books/{}.json'.format(parameter))
        with books_target.open('w') as outfile:
            json.dump(sync['books'], outfile)

        for entry in sync['sport_leagues']:
            sport = entry['sport']
            league = entry['league']
            
            output_file = '{}_{}.json'.format(sport, parameter)
            if len(league) > 0:
                output_file = '{}_{}_{}.json'.format(sport, league, parameter)
            
            games_target = luigi.LocalTarget('./data/odds_shark/games/{}'.format(output_file))
            with games_target.open('w') as outfile:
                json.dump(entry['games'], outfile)

            lines_target = luigi.LocalTarget('./data/odds_shark/lines/{}'.format(output_file))
            with lines_target.open('w') as outfile:
                json.dump(entry['game_lines'], outfile)
        

        ## completed,
        input_path.remove()


if __name__ == '__main__':
    luigi.run()