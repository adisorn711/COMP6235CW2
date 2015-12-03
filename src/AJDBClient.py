__author__ = 'Adisorn'
from pymongo import MongoClient
from AJFileHandler import AJFileHandler
import json
from geopy.geocoders import Nominatim

HOST = 'localhost'
PORT = 27017
DATABASE_NAME = 'microblog'
COLLECTION_NAME = 'blogstatus'
TEMP_FILE_SUFFIX = '.flag'
READ_DIR = '../dataset_clean'
WRITE_DIR = '../dataset_imported'

class AJDBClient(object):

    def __init__(self):
        url = "mongodb://{}:{}/".format(HOST, PORT)
        self.__client = MongoClient(url)
        self.__db = None
        self.__fileHandler = AJFileHandler()
        self.__bulk = None
        self.__cols = []

    def find(self, predicate={}):
        res = []
        cursor = self.collection.find(predicate)
        for obj in cursor:
            res.append(obj)

        return res

    def count(self):
        res = self.collection.count()
        return res

    def uniqueUsers(self):
        res = self.collection.distinct('id_member')
        #print('type of res: {}'.format(type(res)))
        return len(res)

    def top10Published(self):
        msgCount = float(self.count())
        res = self.collection.aggregate([{'$group': {'_id': "$id_member", 'count': {'$sum': 1}}},
                                        {'$sort': {'count': -1}},
                                        {'$limit': 10}])

        count = 0
        for doc in res:
            count += int(doc['count'])

        return (float(count)/msgCount)*100.0

    def meanLengthOfMessages(self):
        msgCount = float(self.count())
        res = self.collection.map_reduce('function(){emit("textLength",this.text.length)}'
                                        , 'function(key, values){ return Array.sum(values)}'
                                        , "results", query={})
        totalLength = 0.0
        for doc in res.find():
            totalLength = float(doc['value'])
            break

        return totalLength/msgCount

    def getLocationOfLargestPublishedMessages(self):
        res = self.collection.aggregate([{'$group': {'_id': {'lat': "$geo_lat", 'long': "$geo_lng"},
                                        'count': {'$sum': 1}}},
                                        {'$sort': {'count': -1}},
                                        {'$limit': 1}],
                                        allowDiskUse=True)
        dct = {}
        lat = ''
        lng = ''
        count = 0
        valid = False
        for doc in res:
            lat = doc['_id']['lat']
            lng = doc['_id']['long']
            count = int(doc['count'])
            valid = True

        if valid:
            geolocator = Nominatim()
            location = geolocator.reverse('{}, {}'.format(lat, lng))
            dct['loc'] = [lat, lng]
            dct['address'] = location.address
            dct['count'] = count
            return dct

        return None

    def use(self, db_name):
        self.db = self.__client[db_name]

    def importFromFile(self, file_name, forced_update=False):
        fileNameExtension = file_name + TEMP_FILE_SUFFIX
        if not forced_update and self.file_handler.fileAlreadyImported(fileNameExtension):
            print("We already have your dataset imported. "
                  "If you want to update with latest dataset, please specify 'forced_update' to True.")
            return None

        print("Please wait, we are importing dataset for you. \nIt won't take long.")
        self.collection.drop()

        dataset_path = self.file_handler.appendStringWithPath(READ_DIR, file_name)
        if self.file_handler.fileExists(dataset_path):
            self.file_handler.loadFile(dataset_path, self.loadCallback, self.loadCompletion)

        #create flag file
        writeFilePath = self.file_handler.appendStringWithPath(WRITE_DIR, fileNameExtension)
        out = open(writeFilePath, 'w+')
        out.close()
        print("Importing finished.")

    def loadCallback(self, objs, isHeader):
        if isHeader:
            headers = []
            for i in range(0, len(objs)):
                headers.append(objs[i].replace('\n', ''))

            self.__cols = headers
        else:
            objs[len(objs)-1].replace('\n', '')
            mongoObj = self.dictFromCSVFormat(self.__cols, objs)
            self.bulk.insert(mongoObj)
        #print('Read input: {}'.format(obj))

    def loadCompletion(self):
        res = self.bulk.execute()

    def close(self):
        self.__client.close()

    def dictFromCSVFormat(self, headers, values):
        dct = {}

        for i in range(0, len(headers)):
            dct[headers[i]] = values[i]
            if headers[i] == 'text':
                text = dct[headers[i]]
                text2 = unicode(text, errors='replace')
                dct[headers[i]] = text2

        return dct


    @property
    def db(self):
        return self.__db

    @db.setter
    def db(self, new_db):
        self.__db = new_db

    @property
    def collection(self):
        if (self.db == None):
            print('Please call "use(db_name)" before using client')
            return None
        return self.db[COLLECTION_NAME]

    @property
    def defaultDatabase(self):
        return DATABASE_NAME

    @property
    def file_handler(self):
        return self.__fileHandler

    @property
    def bulk(self):
        if self.__bulk == None:
            self.__bulk = self.collection.initialize_unordered_bulk_op()

        return self.__bulk