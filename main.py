import csv
import re
from pymongo import MongoClient
from datetime import datetime
from pprint import pprint


def read_data(csv_file, collection):
    with open(csv_file, encoding='utf8') as csvfile:
        reader = csv.DictReader(csvfile)
        reader_copy = list(reader)
        for line in reader_copy:
            line['Цена'] = int(line['Цена'])
            new_date = str(line['Дата'] + ' 2018').replace('.', ' ')
            line['Дата'] = datetime.strptime(new_date, '%d %m %Y')
        collection.insert_many(list(reader_copy))


def find_cheapest(db):
    for line in db.find().sort('Цена'):
        print(line)


def sort_by_date(db):
    for line in db.find().sort('Дата'):
        print(line)


def find_by_name(name, db):
    regex = re.compile('[а-яa-z\\s1-9-]*' + name + '[а-яa-z\\s1-9-]*', re.I)
    for item in db.find({'Исполнитель': regex}).sort('Цена', 1):
        print(item)


if __name__ == '__main__':
    mongo_client = MongoClient()
    artists_db = mongo_client['hm_mongo3']
    artists_collection2 = artists_db['artists_collection2']
    read_data('data/artists.csv', artists_collection2)
    find_cheapest(artists_collection2)
    find_by_name('t', artists_collection2)
    sort_by_date(artists_collection2)
