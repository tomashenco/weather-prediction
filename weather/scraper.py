import datetime
import requests
import bs4
import numpy as np
import pandas as pd

class Scraper:
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
        # Calculate delta and generate a list from start to end date
        delta = (end_date - start_date).days + 1
        for date in (start_date + datetime.timedelta(i) for i in range(delta)):
            print(self.__scrap_day(date))


if __name__ == '__main__':
    s = Scraper()
    s.scrap_period('1-3-2010', '3-3-2010')
