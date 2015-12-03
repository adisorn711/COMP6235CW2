__author__ = 'Adisorn'
import os.path
import csv

DEFAULT_PATH = '../dataset/'
CACHE_DIR = '../dataset_imported'

class AJFileHandler(object):

    def __init__(self):
        pass

    def fileExists(self, file_name):
        return os.path.exists(file_name)

    def loadFile(self, file_name, cb, completion):
        file = open(file_name, 'rU')
        #keys = file.readline().split(',')
        reader = csv.reader(file)
        i = 0
        for readLine in reader:
            cb(readLine, i == 0)
            """
            #if i >= 1: break
            if cb != None:
                dct = {}
                values = readLine.split(',')
                for i in range(0, len(keys)):
                    dct[keys[i]] = values[i]

                #convert unicode
                text = dct['text']
                convertedText = unicode(text, errors = 'replace')
                #print(convertedText)
                dct['text'] = convertedText

                cb(dct)
            """
            i += 1

        completion()

    def fileAlreadyImported(self, file_name):
        cur_path = self.appendStringWithPath(CACHE_DIR, file_name)
        return os.path.exists(cur_path)

    def workingDirectory(self):
        return os.path.dirname(os.path.abspath(__file__)) + '/'

    def appendStringWithPath(self, s1, s2):
        return s1 + '/' + s2

    def getFileExtension(self, file_name):
        arr = []
        idx = file_name.index('.')
        if idx != None:
            arr.append(file_name[0:idx])
            arr.append(file_name[idx+1:])
