__author__ = 'Adisorn'
from pymongo import MongoClient
from AJFileHandler import AJFileHandler
from geopy.geocoders import Nominatim
from datetime import datetime
from bson.code import Code

HOST = 'localhost'
PORT = 27017
DATABASE_NAME = 'microblog'
COLLECTION_NAME = 'blogstatus'
TEMP_FILE_SUFFIX = '.flag'
READ_DIR = '../dataset_clean'
WRITE_DIR = '../dataset_imported'

MAP_MEANLENGTH_STR = """
function(){
    emit(this.id, this.text.length)
}
"""

RED_MEANLENGTH_STR = """
function(key, values){
    return Array.sum(values)
}
"""

MAP_HASHTAG_STR = """
function(){
    var numTags = (this.text.match(/#/g) || []).length;
    emit(this.id, numTags)
}
"""

RED_HASHTAG_STR = """
function(key, values){
    return Array.sum(values)
}
"""

MAP_NGRAM_STR = """
function(){
    var punctuationless = this.text.replace(/[.,-\/#!$%\^&\*;:{}=\-_`~()]/g,"");
    var finalString = punctuationless.replace(/\s{2,}/g," ");

    var words = finalString.split(' ');
    var i;
    for (var i=0; i < words.length; i++) {
        var word = words[i].trim();
        if (word.length > 0) {
            emit(word, 1);
        }
    }
}
"""

MAP_BIGRAM_STR = """
function(){
    var punctuationless = this.text.replace(/[.,-\/#!$%\^&\*;:{}=\-_`~()]/g,"");
    var finalString = punctuationless.replace(/\s{2,}/g," ");

    var words = finalString.split(' ');
    var i;
    for (var i=0; i < words.length-1; i++) {
        var twoWords = words[i] + " " + words[i+1];
        emit(twoWords, 1);
    }
}
"""

RED_NGRAM_STR = """
function(key, values){
    return Array.sum(values)
}
"""


class AJDBClient(object):

    def __init__(self):
        url = "mongodb://{}:{}/".format(HOST, PORT)
        self.__client = MongoClient(url)
        self.__db = None
        self.__fileHandler = AJFileHandler()
        self.__bulk = None
        self.__cols = []

        self.__meanLenResult = None
        self.__hashtagResult = None
        self.__unigramResult = None
        self.__bigramResult = None

    def find(self, predicate={}):
        res = []
        cursor = self.collection.find(predicate)
        for obj in cursor:
            res.append(obj)

        return res

    def count(self):
        res = self.collection.count()
        return res

    def getUniqueUsers(self):
        res = self.collection.distinct('id_member')
        #print('type of res: {}'.format(type(res)))
        return len(res)

    def getTop10Published(self):
        msgCount = float(self.count())
        res = self.collection.aggregate([{'$group': {'_id': "$id_member", 'count': {'$sum': 1}}},
                                        {'$sort': {'count': -1}},
                                        {'$limit': 10}])

        count = 0
        for doc in res:
            count += int(doc['count'])

        return (float(count)/msgCount)*100.0

    def getMinAndMaxDate(self):
        minDateRes = self.collection.find().sort('timestamp', 1).limit(1)
        maxDateRes = self.collection.find().sort('timestamp', -1).limit(1)

        minDate = None
        maxDate = None
        for doc in minDateRes:
            minDate = doc['timestamp']

        for doc in maxDateRes:
            maxDate = doc['timestamp']

        return minDate, maxDate

    def getMeanTimeDelta(self):
        minDate , maxDate = self.getMinAndMaxDate()
        print(minDate)

        date1 = datetime.strptime(minDate, "%Y-%m-%d %H:%M:%S")
        date2 = datetime.strptime(maxDate, "%Y-%m-%d %H:%M:%S")
        delta = date2 - date1

        deltaTime = delta.total_seconds()/self.count()
        return deltaTime

    def getMeanLengthOfMessages(self):
        if self.__meanLenResult is None:
            mapFunc = Code(MAP_MEANLENGTH_STR)
            redFunc = Code(RED_MEANLENGTH_STR)
            self.__meanLenResult = self.collection.map_reduce(mapFunc, redFunc, "meanTimeResults", query={})

        meanRes = self.__meanLenResult.aggregate([{'$group': {'_id': "null", 'average': {'$avg': "$value"}}}], allowDiskUse=True)

        meanLength = 0.0
        for doc in meanRes:
            meanLength = float(doc['average'])

        return meanLength

    def getUnigramAndBigram(self):
        if self.__unigramResult is None:
            mapFunc = Code(MAP_NGRAM_STR)
            redFunc = Code(RED_NGRAM_STR)
            self.__unigramResult = self.collection.map_reduce(mapFunc, redFunc, "unigramResults", query={})

        if self.__bigramResult is None:
            mapFunc = Code(MAP_BIGRAM_STR)
            redFunc = Code(RED_NGRAM_STR)
            self.__bigramResult = self.collection.map_reduce(mapFunc, redFunc, "bigramResults", query={})


        unigRes = self.__unigramResult.find().sort('value', -1).limit(10)
        bigramRes = self.__bigramResult.find().sort('value', -1).limit(10)

        unigramRecords = []
        for doc in unigRes:
            dct = {}
            dct['word'] = doc['_id']
            dct['count'] = int(doc['value'])
            unigramRecords.append(dct)

        bigramRecords = []
        for doc in bigramRes:
            dct = {}
            dct['word'] = doc['_id']
            dct['count'] = int(doc['value'])
            bigramRecords.append(dct)

        return unigramRecords, bigramRecords


    def getNumberOfHashtags(self):
        if self.__hashtagResult is None:
            mapFunc = Code(MAP_HASHTAG_STR)
            redFunc = Code(RED_HASHTAG_STR)
            self.__hashtagResult = self.collection.map_reduce(mapFunc, redFunc, "hashtagResults", query={})

        hashtagRes = self.__hashtagResult.aggregate([{'$group': {'_id': "null", 'total': {'$sum': "$value"}}}], allowDiskUse=True)

        nHashtags = 0.0
        for doc in hashtagRes:
            nHashtags = float(doc['total'])

        return nHashtags/self.count()


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
        if self.db is None:
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
        if self.__bulk is None:
            self.__bulk = self.collection.initialize_unordered_bulk_op()

        return self.__bulk