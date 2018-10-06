import sys
import json
import requests
import datetime
import odds_parser

def scrap(configuration):

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

    sync = {
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

    return sync



def load_configuration(configuration_directory):
    configuration = {}
    configuration_path = r'{}\config.json'.format(configuration_directory)
    with open(configuration_path, 'r') as config_output:
        configuration = json.loads(config_output.read())
    return configuration



def convert_date_to_filename(date):
    return date.replace(' ', '_').replace(':', '_').replace('.', '_')


