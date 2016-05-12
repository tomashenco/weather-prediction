import datetime
import requests
import bs4
import pandas as pd
import re
import pickle


class Scraper:
    # Class for scraping data from weatheronline.co.uk for further analysis
    def __init__(self):
        # number for 1st of Jan 2010
        self.__begin_number = 1262347200
        # 1800 per period (half an hour) so 48 periods = day gap
        self.__day_gap = 86400
        # only the number after DATE= changes
        self.__empty_path = ['http://www.weatheronline.co.uk/weather/maps/current?LANG=en&DATE=',
                             '&CONT=ukuk&LAND=UK&KEY=UK&SORT=2&UD=0&INT=06&TYP=temperatur&ART='
                             'tabelle&RUBRIK=akt&R=310&CEL=C&SI=mph']
        self.__date = datetime.date(2010, 1, 1)
        self.__stations = ['Aberdaron (94 m)', 'Aberdeen Airport (69 m)', 'Aberporth (134 m)',
                           'Alderney Channel Is (71 m)', 'Altnaharra (80 m)', 'Andrewsfield Aerodrome (87 m)',
                           'Aviemore (228 m)', 'Ballykelly (5 m)', 'Ballypatrick Fst (156 m)',
                           'Baltasound (15 m)', 'Bedford (84 m)', 'Belfast Intl. Airport (68 m)',
                           'Belfast/Harbour (5 m)', 'Benbecula (6 m)', 'Benson (57 m)',
                           'Biggin Hill (183 m)', 'Bingley (267 m)', 'Birmingham Airport (99 m)',
                           'Blackpool Airport (10 m)', 'Boulmer (27 m)', 'Bournemouth (10 m)',
                           'Bridlington (19 m)', 'Bristol / Lulsgate (189 m)', 'Cairngorms (1245 m)',
                           'Camborne (88 m)', 'Cambridge (15 m)', 'Capel Curig (215 m)',
                           'Cardiff Airport (67 m)', 'Cardinham (199 m)', 'Carlisle (27 m)',
                           'Carlisle Lake District Airport (58 m)', 'Castlederg (50 m)', 'Charlwood (58 m)',
                           'Charterhall (111 m)', 'Church Lawford (106 m)', 'Coleshill (96 m)',
                           'Coningsby (6 m)', 'Coventry Airport (82 m)', 'Cranfield Airport (111 m)',
                           'Credenhill (76 m)', 'Crosby (9 m)', 'Donna Nook (8 m)',
                           'Drumalbin (245 m)', 'Dundee/Riverside (4 m)', 'Dundrennan (113 m)',
                           'Dunkeswell Airport (253 m)', 'Durham Tees Valley Airport (37 m)', 'East Midlands Airport (94 m)',
                           'Edinburgh Airport (41 m)', 'Edinburgh Gogarbank (57 m)', 'Eglinton Airport (9 m)',
                           'Eskdalemuir (242 m)', 'Exeter Airport (30 m)', 'Fair Isle (57 m)',
                           'Farnborough (64 m)', 'Filton (59 m)', 'Foula (13 m)',
                           'Fylingdales (262 m)', 'Gatwick Airport (62 m)', 'Glasgow Airport (8 m)',
                           'Glasgow Bishopton (59 m)', 'Glen Ogle (564 m)', 'Glenanne (160 m)',
                           'Gloucestershire Airport (29 m)', 'Gravesend Broadness (10 m)', 'Great Dun Fell (847 m)',
                           'Guernsey Airport (101 m)', 'Hawarden (9 m)', 'Herstmonceux (52 m)',
                           'High Wycombe (205 m)', 'Humberside (31 m)', 'Inverness Airport (9 m)',
                           'Islay/Port Ellen (17 m)', 'Isle of Man Airport (16 m)', 'Isle of Portland (52 m)',
                           'Jersey Airport (84 m)', 'Kenley (170 m)', 'Keswick (81 m)',
                           'Kinloss (5 m)', 'Kirkwall Airport (21 m)', 'Lake Vyrnwy (359 m)',
                           'Lakenheath (10 m)', 'Langdon Bay (117 m)', 'Larkhill (133 m)',
                           'Leeds Bradford Intl. Airport (208 m)', 'Leeds East Airport (8 m)', 'Leeming (32 m)',
                           'Lerwick (84 m)', 'Leuchars (10 m)', 'Linton-On-Ouse (14 m)',
                           'Liscombe (347 m)', 'Liverpool AP (26 m)', 'Loch Glascarnoch (264 m)',
                           'Loftus Samos (159 m)', 'London City Airport (5 m)', 'London Heathrow Airport (25 m)',
                           'London Southend Airport (15 m)', 'London Stansted Airport (106 m)', 'Lossiemouth (6 m)',
                           'Lough Fea (225 m)', 'Luton Airport (160 m)', 'Machrihanish (10 m)',
                           'Manchester (69 m)', 'Manston (50 m)', 'Manston South East (54 m)',
                           'Marham (21 m)', 'Middle Wallop (91 m)', 'Mildenhall Royal Air Force (10 m)',
                           'Milford Haven (32 m)', 'MoD Boscombe Down (126 m)', 'MoD Lyneham (156 m)',
                           'Mumbles (32 m)', 'Newcastle Airport (81 m)', 'Northolt (39 m)',
                           'Norwich Weather Centre (37 m)', 'Nottingham/Watnall (117 m)', 'Pembrey Burrows (6 m)',
                           'Pershore (31 m)', 'Plymouth City Airport (25 m)', 'Plymouth MtBatten (50 m)',
                           'Portglenone (65 m)', 'Prestwick (27 m)', 'RAF Brize Norton (88 m)',
                           'RAF Cottesmore (142 m)', 'RAF Dishforth (33 m)', 'RAF Holbeach (2 m)',
                           'RAF Little Rissington (210 m)', 'RAF Odiham (118 m)', 'RAF Tain (4 m)',
                           'RAF West Freugh (10 m)', 'RNAS Culdrose (84 m)', 'Redesdale Camp (212 m)',
                           'Rhyl (76 m)', 'Royal Marines Base Chivenor (6 m)', 'Scampton (57 m)',
                           'Scatsa/Shetland Island (22 m)', 'Scilly St Mary (30 m)', 'Sennybridge (307 m)',
                           'Shap (249 m)', 'Shawbury (72 m)', 'Shoeburyness (3 m)',
                           'Shoreham Airport (2 m)', 'Skye/Lusa (18 m)', 'Solent MRSC (13 m)',
                           'South Uist Range (4 m)', 'Southampton Airport (9 m)', 'Spadeadam (325 m)',
                           'Spadeadam II (286 m)', 'St Angelo (47 m)', 'St Athan (49 m)',
                           'St Bees Head (123 m)', "St. Catherine's Point (24 m)", 'Stornoway (15 m)',
                           'Strathallan (35 m)', 'Sule Skerry (12 m)', 'Sumburgh Cape (5 m)',
                           'Thorney Island (3 m)', 'Tiree Island (12 m)', 'Topcliffe (28 m)',
                           'Trawscoed (62 m)', 'Tulloch Bridge (236 m)', 'Valley (10 m)',
                           'Waddington (68 m)', 'Wainfleet (5 m)', 'Walney (14 m)',
                           'Warcop Range (227 m)', 'Wattisham (89 m)', 'Weybourne (20 m)',
                           'Wick (36 m)', 'Wittering (84 m)', 'Woodford (88 m)',
                           'Yeovil (20 m)']

        self.__search_float = re.compile(r'\d*\.?\d+')

    def __get_link(self, date):
        """
        Generate number appropriate for that date by taking starting point (1-1-2010)
        and adding day gap for each day of differance
        :param date: desired date
        :return: link to the website with data for that date
        """
        date_number = self.__begin_number + (date - self.__date).days * self.__day_gap
        # Combine the link
        link = self.__empty_path[0] + str(date_number) + self.__empty_path[1]
        return link

    def __scrap_day(self, date):
        """
        Scraps the data for a single day and pass the data
        :param date: desired date
        :return: 2 lists: station names and temperature for that date
        """
        link = self.__get_link(date)
        # Get html and parse it to bs
        page = requests.get(link)
        soup = bs4.BeautifulSoup(page.text, 'html.parser')
        # Scrap only data for the temperature, discard first 2 rows of headers
        table = [item.text for item in soup.find_all('td')[2:]]
        # Odd is tags, even is temperature
        tags, temperatures = table[::2], table[1::2]
        return tags, temperatures

    def scrap_period(self, start, end):
        """
        Scrap data for a period of time
        :param start: start date
        :param end: end date
        :return: pandas table of temperature
        """
        # Parse the date as datetime object
        start_date = datetime.datetime.strptime(start, '%d-%m-%Y').date()
        end_date = datetime.datetime.strptime(end, '%d-%m-%Y').date()
        # Create empty dictionary for storage
        storage = dict()

        # Calculate delta and generate a list from start to end date
        delta = (end_date - start_date).days + 1
        for date in (start_date + datetime.timedelta(i) for i in range(delta)):
            # Get data for a single day
            stations, temperatures = self.__scrap_day(date)
            # Change to dictionary for faster access
            results = dict(zip(stations, temperatures))

            # Because the list of stations from web could be incomplete, we must iterate through the whole list
            # then the result is added or specified as None if it doesn't exist
            for key in self.__stations:
                if key not in results:
                    if key not in storage:
                        storage[key] = [None]
                    else:
                        storage[key].append(None)
                else:
                    value = self.__search_float.search(results[key]).group(0)
                    if key not in storage:
                        storage[key] = [value]
                    else:
                        storage[key].append(value)

        # Dates as row names
        dates = pd.date_range(start_date, periods=delta)
        # Create data frame
        df = pd.DataFrame(storage, index=dates)
        return df


if __name__ == '__main__':
    # s = Scraper()
    # df = s.scrap_period('2-1-2011', '2-1-2011')
    # with open('data.pickle', 'wb') as f:
    #     pickle.dump(df, f, pickle.HIGHEST_PROTOCOL)
    with open('data.pickle', 'rb') as f:
        df = pickle.load(f)

    print(df)
