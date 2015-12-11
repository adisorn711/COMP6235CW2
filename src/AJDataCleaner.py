__author__ = 'Adisorn'
from AJFileHandler import AJFileHandler


class AJDataCleaner:

    def __init__(self, read_dir, write_dir):
        self.__readDir = read_dir
        self.__writeDir = write_dir
        self.__fileHandler = AJFileHandler()
        self.cols = []
        self.__tempFile = None
        self.__pendingData = []

    def cleanData(self, file_name, forced_update=False):
        file_path = self.__fileHandler.appendStringWithPath(self.readDir, file_name)
        if not self.__fileHandler.fileExists(file_path):
            return False

        old_path = self.__fileHandler.appendStringWithPath(self.writeDir, file_name)
        if not forced_update and self.__fileHandler.fileExists(old_path):
            return True

        self.__tempFile = self.openFile(self.__fileHandler.appendStringWithPath(self.writeDir, file_name))
        self.__fileHandler.loadFile(file_path, self.loadCallback, self.completion)
        return True

    def loadCallback(self, obj, header):
        if header:
            headers = []
            for i in range(0, len(obj)):
                headers.append(obj[i].replace('\n', ''))

            self.cols = headers
            self.writeToFile(','.join(headers) + '\n')
        elif len(self.cols) == len(obj):
            values = [obj]

            if len(self.__pendingData) > 0:
                result = []
                first3Data = self.__pendingData[:3]
                text = self.__pendingData[3:-2]
                locations = self.__pendingData[-2:]

                result.extend(first3Data)
                result.extend(' '.join(text))
                result.extend(locations)
                values.append(result)

            for k in range(0, len(values)):
                doc = values[k]

                newVals = []
                for i in range(0, len(doc)):
                    key = self.cols[i]
                    value = doc[i]

                    if key == 'id':
                        pass
                    elif key == 'id_member':
                        v = abs(long(value))
                        value = str(v)
                    elif key == 'timestamp':
                        pass
                    elif key == 'text':
                        pass
                    elif key == 'geo_lat':
                        value = value.replace('\n', '')
                    elif key == 'geo_lng':
                        value = value.replace('\n', '')

                    newVals.append(value)

            self.writeToFile(','.join(newVals) + '\n')
            self.__pendingData = []

        elif len(self.cols) == len(obj):
            self.__pendingData.extend(obj)

    def completion(self):
        self.__tempFile.close()

    def openFile(self, file_path):
        f = open(file_path, 'w+')
        return f

        #self.bulk.insert(obj)

    def writeToFile(self, obj):
        self.__tempFile.write(obj)

    @property
    def readDir(self):
        return self.__readDir

    @readDir.setter
    def readDir(self, dr):
        self.__readDir = dr

    @property
    def writeDir(self):
        return self.__writeDir

    @writeDir.setter
    def writeDir(self, dr):
        self.__writeDir = dr

