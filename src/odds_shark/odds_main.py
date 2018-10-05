import sys
import json
import requests
import datetime
import odds_parser

def scrap(configuration_directory, output_directory):

    def get_uri(sport, league):
        uri = r'https://www.oddsshark.com/{}/'.format(sport)
        if len(league) > 0:
            uri += r'{}/'.format(league)
        return uri + r'odds'

    def merge_op_books(current_books, cached_books):
        for op_book in current_books:
            if not (op_book['id'] in [ item['id'] for item in cached_books ]):
                cached_books.append(op_book)
        return cached_books

    configuration = {}
    configuration_path = r'{}\config.json'.format(configuration_directory)
    with open(configuration_path, 'r') as config_output:
        configuration = json.loads(config_output.read())

    sync = {
        'id': str(datetime.datetime.now()),
        'books': [],
        'sport_leagues': []
    }

    for sport in configuration['sports']:
        for league in sport['leagues']:
            
            ## build sync obj,
            sport_league_sync = {
                'sport': sport['name'],
                'league': league
            }
            
            ## pull sport / league / odds page,
            uri = get_uri(sport_league_sync['sport'], sport_league_sync['league'])
            response = requests.get(uri)

            ## assert all is good,
            assert response.status_code == 200

            parser = odds_parser.OddsParser(response.content)
            books, games, lines = parser.parse()
            
            ## update global books list,
            sync['books'] = merge_op_books(books, sync['books'])

            ## update sports league sync obj,
            sport_league_sync['games'] = games
            sport_league_sync['game_lines'] = lines

            sync['sport_leagues'].append(sport_league_sync)



    file = sync['id'].replace(' ', '_').replace(':', '_').replace('.', '_')
    path = '{}\{}.json'.format(output_directory, file)
    with open(path, 'w') as sync_output:
        json.dump(sync, sync_output)



if __name__ == "__main__":
    
    number_of_parameters = len(sys.argv)

    if number_of_parameters > 1 or number_of_parameters == 2:
        configuration_directory = sys.argv[1]
    else:
        configuration_directory = r'.\configs\odds_shark'

    if number_of_parameters == 3:
        output_directory = sys.argv[2]
    else:
        output_directory = r'.\data\odds_shark'

    scrap(configuration_directory, output_directory)