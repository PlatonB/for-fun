print('''
Генератор небольших MongoDB-коллекций случайных значений.
Автор: Платон Быкадоров (platon.work@gmail.com), 2019.
Версия: V2.0.
Лицензия: GNU General Public License version 3.
Поддержать автора: https://money.yandex.ru/to/41001832285976

Перед запуском скрипта нужно установить
СУБД MongoDB Community Edition, выбрав
инструкцию для вашего дистрибутива:
https://docs.mongodb.com/manual/installation/#mongodb-community-edition-installation-tutorials

Также нужно установить MongoDB-драйвер PyMongo
с Python-привязками для архиватора Zstandard:
pip3 install pymongo[zstd] --user

После установки требуемых
компонентов советую перезагрузиться.

При каждом запуске скрипт
стирает предыдущую коллекцию.

По сгенерированным данным можно
тренироваться делать запросы,
используя синтаксис PyMongo/MongoDB.

Краткое введение в PyMongo:
https://api.mongodb.com/python/current/tutorial.html

Материалы для более глубокого изучения PyMongo:
https://api.mongodb.com/python/current/

В конце скрипт выведет код-пасхалку
в качестве образца запроса к коллекции.
''')

from pymongo import MongoClient, IndexModel, ASCENDING
from random import randint, choice
from string import ascii_letters, digits, ascii_lowercase
from bson.decimal128 import Decimal128
from decimal import Decimal, ROUND_HALF_UP
from pprint import pprint

#Подключение к MongoDB-серверу.
client = MongoClient(compressors='zstd')

#Создание базы и коллекции
#(если они ещё не созданы).
random_db = client.random_db
random_coll = random_db.random_coll

#Если коллекция, полученная
#при предыдущих запусках,
#уже есть, то будет удалена.
if 'random_coll' in random_db.collection_names():
        random_coll.drop()
        
#Генерация списка словарей с
#данными разных типов как можно
#более заковыристым способом:).
fragment = [{'integer': randint(0, 100),
             'string': ''.join([choice(ascii_letters + digits) for j in range(randint(1, 10))]),
             'decimal': Decimal128((Decimal(randint(1, 49)) / Decimal(randint(50, 100))).quantize(Decimal('1.' + '0' * randint(1, 5)), ROUND_HALF_UP)),
             'list': [choice([''.join([choice(ascii_lowercase) for k in range(randint(1, 5))]), randint(-10000, 10000)]) for j in range(randint(1, 5))]} for i in range(randint(10, 50))]

#Размещение этого списка в коллекцию.
random_coll.insert_many(fragment)

#Индексация по каждому полю по-отдельности.
indexes = [IndexModel([(key, ASCENDING)]) for key in fragment[0].keys()]
random_coll.create_indexes(indexes)

print('Индексы:')
pprint(random_coll.index_information())

print('\nДанные:')
for doc in random_coll.find():
        pprint(doc)
        print('')
        
print('''
Код испытания счастья:).
Вывелось число - повезло.
Вывелось число более,
чем один раз за один
запрос - супервезение:).

for doc in random_coll.find({'integer': randint(0, 100)}):
        pprint(doc['integer'])
''')

#client.close()
