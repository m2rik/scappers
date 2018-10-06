import uuid
import datetime

from bs4 import BeautifulSoup

class OddsParser:
    
    def __init__(self, content):
        self.soup = BeautifulSoup(content, 'html.parser')

    def parse(self):
        return self.get_books(), self.get_games(), self.get_lines(games)
    
    def get_books(self):   
        return [
            {
                'name': item.find_all('img')[0]['alt'],
                'is_vegas': 'vegas' in item['class'],
                'no_vegas': 'no-vegas' in item['class'],
                'id': 'op-{}'.format(item.find_all('img')[0]['alt'].lower())
            }
        for item in self.soup.select('div.op-book-wrapper div.op-book-header') ]

    def get_games(self):
        
        ## utility,
        def pad_number(n):
            formatted = '0{}'.format(n)
            if len(formatted) > 2:
                return formatted[1:]
            return formatted

        def determine_year(last_month, current_month, year):
            if not last_month == None and (int(last_month) > int(current_month)):
                year += 1
            return year
    
        month_mappings = {
            "January": "01",
            "February": "02",
            "March": "03",
            "April": "04",
            "May": "05",
            "June": "06",
            "July": "07",
            "August": "08",
            "September": "09",
            "October": "10",
            "November": "11",
            "December": "12"
        }

        year = datetime.datetime.now().year
        
        ## int,
        last_month = None
        
        games = [ ]
        for search in self.soup.select('div.op-team-data-wrapper > div'):

            cls = search['class']
            if 'op-separator-bar' in cls:
                op_date_attribute = search.get('data-op-date')
                if op_date_attribute == None:
                    continue
                
                date_string = eval(op_date_attribute)['full_date']
                date_parts  = date_string.split(' ')
                full_day    = date_parts[0]

                month      = month_mappings[date_parts[1]]
                year       = determine_year(last_month, month, year)
                last_month = month
                
                day  = pad_number(int(date_parts[2]))
                date = '{}-{}-{}'.format(year, day, month)

            if 'op-matchup-wrapper' in cls:

                time = str(search.find(class_='op-matchup-time').text).encode('ascii', 'ignore').decode('ascii')

                home = eval(search.find(class_='op-team-top')['data-op-name'])
                away = eval(search.find(class_='op-team-bottom')['data-op-name'])

                games.append({
                    'game_id': uuid.uuid4().hex,
                    'date': date,
                    'time': '{} {}m'.format(time[:-1], time[-1:]),
                    'home_full': home['full_name'],
                    'home_short': home['short_name'],
                    'away_full': away['full_name'],
                    'away_short': away['short_name']
                })
        
        return games

    def get_lines(self, games):
        
        def is_empty_line(game_line):
            if len(game_line['away']) > 0:
                return len(game_line['away'][0]['moneyline']) == 0
            return True

        def parse_moneyline(moneyline):
            val = eval(moneyline)['fullgame']
            if len(val) > 0:
                return (val[0], val[1:])
            return ('', '')

        lines = []
        game_index = 0
        for search in self.soup.select('div#op-results > div'):
            cls = search['class']
            
            if 'op-item-row-wrapper' in cls:
                for op_search in search.select('div.op-item-wrapper'):

                    home = []
                    away = []
                    draw = []

                    i = 0
                    op_items = op_search.select('div.op-item')
                    for op_item in op_items:

                        op_item_cls = op_item['class'].copy()
                        if i == 0:
                            site = op_item_cls.pop()

                        obj = {}

                        if 'op-spread' in op_item_cls:
                            obj['type'] = 'spread'
                            obj['total'] = eval(op_item['data-op-total'])['fullgame']
                            obj['moneyline_sign'], obj['moneyline'] = parse_moneyline(op_item['data-op-moneyline'])

                            if i == 0:
                                home.append(obj)
                            else:
                                away.append(obj)

                        if 'spread-price' in op_item_cls:
                            obj['type'] = 'spread-price'
                            obj['info_sign'], obj['info_price'] = parse_moneyline(op_item['data-op-info'])

                            if i == 1:
                                obj['price_sign'], obj['price'] = parse_moneyline(op_item['data-op-overprice'])
                                home.append(obj)
                            else:
                                obj['price_sign'], obj['price'] = parse_moneyline(op_item['data-op-underprice'])
                                away.append(obj)

                        if 'op-draw' in op_item_cls:
                            obj['type'] = 'draw'
                            obj['moneyline_sign'], obj['moneyline'] = parse_moneyline(op_item['data-op-moneyline'])
                            draw.append(obj)

                        i += 1

                    game_line = {
                        'home': home,
                        'away': away,
                        'draw': draw,
                        'site_id': site,
                        'game_id': games[game_index]['game_id']
                    }

                    if not is_empty_line(game_line):
                        lines.append(game_line);
                    
                game_index += 1

        return lines