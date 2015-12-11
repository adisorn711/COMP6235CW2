__author__ = 'Adisorn'
from AJDBClient import AJDBClient
from AJDataCleaner import AJDataCleaner

def main():
    """
    client = AJDBClient()
    client.use(client.defaultDatabase)

    res = client.importFromFile('microblogDataset_COMP6235_CW2.csv')
    count = client.uniqueUsers()
    print('How many unique users are there? : {} user(s).'.format(count))
    client.close()
    """

    HomeScreen = "Welcome to Coursework 2 of Data Science.\n" \
                 "Please type the choice number to proceed...\n" \
                 "1. Import the dataset to mongoDB\n" \
                 "2. How many unique users are there?\n" \
                 "3. How many tweets (%) did the top 10 users (measured by the number of messages) publish?\n" \
                 "4. What was the earliest and latest date (YYYY-MM-DD HH:MM:SS) that a message was published?\n" \
                 "5. What is the mean time delta between all messages?\n" \
                 "6. What is the mean length of a message?\n" \
                 "7. What are the 10 most common unigram and bigram strings within the messages?\n" \
                 "8. What is the average number of hashtags (#) used within a message?\n" \
                 "9. Which area within the UK contains the largest number of published messages?\n" \
                 "10. quit the program\n\n" \
                 "select: "

    GoodBye = "Good Bye !!"
    AnyKeyToContinue = "Press enter to continue..."

    dataCleaner = AJDataCleaner('../dataset/', '../dataset_clean')
    dataCleaner.cleanData('microblogDataset_COMP6235_CW2.csv')


    client = AJDBClient()
    client.use(client.defaultDatabase)
    res = client.importFromFile('microblogDataset_COMP6235_CW2.csv')

    while 1:
        choice = int(input(HomeScreen))

        if choice == 10:
            print(GoodBye)
            break

        print('Your request is being processed. Please wait...')
        if choice == 2:
            count = client.getUniqueUsers()
            print('There are {} unique user(s).\n'.format(count))
        elif choice == 3:
            count = client.getTop10Published()
            print('There are {:.2f} tweet(s)% published by top 10 users.\n'.format(count))
        elif choice == 4:
            minDate, maxDate = client.getMinAndMaxDate()
            print('The earliest date was {}, and the latest date was {}.\n'.format(minDate, maxDate))
        elif choice == 5:
            delta = client.getMeanTimeDelta()
            print('The mean delta time is {:.2f} second(s).\n'.format(delta))
        elif choice == 6:
            meanLen = client.getMeanLengthOfMessages()
            print('Mean length of all messages is {:.2f} character(s).\n'.format(meanLen))
        elif choice == 7:
            unigrams, bigrams = client.getUnigramAndBigram()

            print('These are the 10 most common unigram strings:\n')
            for i in range(len(unigrams)):
                dct = unigrams[i]
                print('{}. "{}" with {} count'.format(i+1, dct['word'], dct['count']))

            print('\nThese are the 10 most common bigram strings:\n')
            for i in range(len(bigrams)):
                dct = bigrams[i]
                print('{}. "{}" with {} count'.format(i+1, dct['word'], dct['count']))

        elif choice == 8:
            count = client.getNumberOfHashtags()
            print('The average number of hashtag(#) is {:.2f} used within a message.\n'.format(count))
        elif choice == 9:
            res = client.getLocationOfLargestPublishedMessages()
            if res == None:
                print("We are sorry. There is no area that contains the largest number of published messages.\n")
            else:
                print('The area in UK that contains the largest number of published {} message(s) '
                      '\nis at location(lat: {}, long: {}) '
                      '\nwhich can be referred to {}\n'.
                      format(res['count'], res['loc'][0], res['loc'][1], res['address']))

        tmp = raw_input(AnyKeyToContinue)

    client.close()

if __name__ == '__main__':main()