import luigi

import json
import odds_helpers

class OddsSharkScrapperTask(luigi.Task):
    now = luigi.Parameter('now')
    

    def output(self):
        directory = './data/odds_shark/downloaded'
        parameter = odds_helpers.convert_date_to_filename(self.now)
        local_path = '{}/{}.json'.format(directory, parameter)
        return luigi.LocalTarget(local_path)


    def run(self):
        configuration = odds_helpers.load_configuration('./configs/odds_shark')
        sync = odds_helpers.scrap(configuration)
        with self.output().open('w') as outfile:
            json.dump(sync, outfile)


if __name__ == '__main__':
    luigi.run()