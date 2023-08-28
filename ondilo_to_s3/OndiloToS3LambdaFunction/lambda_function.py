# -*- coding: utf-8 -*-

import sys
import os

print(os.path.dirname('./'))
sys.path.append(os.path.dirname('./')+'/python-libs')


import logging
import boto3
from ondilo import Ondilo
from datetime import datetime,timedelta
from dateutil import tz
import json

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

FRA = tz.gettz('Europe/Paris')

#lambda handler
def lambda_handler(event, context):

    #write the event in the logs
    print(event)

    #get the bucket name from the environment variable
    myFHStreamName = os.environ['KinesisStreamName']

    #get the secret token name from the environment variable
    mySecretTokenName = os.environ['OndiloTokenSecretName']

    myPeriod = 'day'

    #check if the month parameter exists and is set to true    
    if 'month' in event and event['month'] == 'true':
        print('month parameter is set to true')
        myPeriod = 'month'

    #get the token contained in the secret
    myToken =  eval(boto3.client('secretsmanager').get_secret_value(SecretId=mySecretTokenName)['SecretString'])

    #connect to the Ondilo API
    myClient = Ondilo(myToken)

    #get the list of pools
    myPools = myClient.get_pools()
    
    #print the list of pools
    print(myPools)

    #create the firehose ressource
    myFH = boto3.client('firehose')

    myRecords = []

    #for each pool get all the values from the last 24h and store them    
    for myPool in myPools:
        print('process pool {} data'.format(myPool['id']))

        #temperature
        myTemps = myClient.get_pool_histo(myPool['id'], 'temperature', myPeriod)

        myRecords+= myTemps

        #ph
        myPhs = myClient.get_pool_histo(myPool['id'], 'ph', myPeriod)
        myRecords+= myPhs

        #orp
        myOrps = myClient.get_pool_histo(myPool['id'], 'orp', myPeriod)
        myRecords+= myOrps

        #tds
        myTds = myClient.get_pool_histo(myPool['id'], 'tds', myPeriod)
        myRecords+= myTds

        #battery
        myBatteries = myClient.get_pool_histo(myPool['id'], 'battery', myPeriod)
        myRecords+= myBatteries

        #rssi
        myRssis = myClient.get_pool_histo(myPool['id'], 'rssi', myPeriod)
        myRecords+= myRssis        
        
        for record in myRecords:
            record['id'] = myPool['id']
            record['year'] = record['value_time'].split('T')[0].split('-')[0]
            record['month'] = record['value_time'].split('T')[0].split('-')[1]
            record['day'] = record['value_time'].split('T')[0].split('-')[2]
            

        #This trick is here to ensure we do not write more than 500 records in a single batch
        MaxNumberOfRecords = 499
        myRecordsLen = len(myRecords)
        i=0

        while(i<myRecordsLen/MaxNumberOfRecords):
            myResult = myFH.put_record_batch(DeliveryStreamName=myFHStreamName, Records=[{"Data":json.dumps(temp)} for temp in myRecords[i*MaxNumberOfRecords:i*MaxNumberOfRecords+MaxNumberOfRecords]])
            print(myResult)
            i+=1
