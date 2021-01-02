from time import sleep
from datetime import datetime
import json
import argparse
import threading, time
import logging

from kafka import KafkaProducer
import xml.etree.ElementTree as etree

def getMessageForPost(elem):
    try:
        postTypeId = int(elem.attrib['PostTypeId'])

        if postTypeId != 1 and postTypeId != 2:
            print('[PostId: {0}] Unrecognised PostTypeId: {1}'.format(elem.attrib['Id'], postTypeId))
            return None;

        message = {'Id': int(elem.attrib['Id']),
                   'PostTypeId': postTypeId,
                   'CreationDate': elem.attrib['CreationDate']}

        if 'OwnerUserId' in elem.attrib:
            message['OwnerUserId'] = int(elem.attrib['OwnerUserId']);
        if 'OwnerDisplayName' in elem.attrib:
            message['OwnerDisplayName'] = elem.attrib['OwnerDisplayName'];
        if 'LastEditorUserId' in elem.attrib:
            message['LastEditorUserId'] = int(elem.attrib['LastEditorUserId']);
        if 'LastActivityDate' in elem.attrib:
            message['LastActivityDate'] = elem.attrib['LastActivityDate'];
        if 'LastEditDate' in elem.attrib:
            message['LastEditDate'] = elem.attrib['LastEditDate'];
        if 'AcceptedAnswerId' in elem.attrib:
            message['AcceptedAnswerId'] = int(elem.attrib['AcceptedAnswerId']);
        if 'ParentId' in elem.attrib:
            message['ParentId'] = int(elem.attrib['ParentId']);
        if 'ViewCount' in elem.attrib:
            message['ViewCount'] = int(elem.attrib['ViewCount']);
        if 'Score' in elem.attrib:
            message['Score'] = float(elem.attrib['Score']);
        if 'Title' in elem.attrib:
            message['Title'] = elem.attrib['Title'];
        if 'Body' in elem.attrib:
            message['Body'] = elem.attrib['Body'];
        if 'Tags' in elem.attrib:
            message['Tags'] = elem.attrib['Tags'];
        if 'ViewCount' in elem.attrib:
            message['ViewCount'] = int(elem.attrib['ViewCount']);
        if 'AnswerCount' in elem.attrib:
            message['AnswerCount'] = int(elem.attrib['AnswerCount']);
        if 'CommentCount' in elem.attrib:
            message['CommentCount'] = int(elem.attrib['CommentCount']);
        if 'FavoriteCount' in elem.attrib:
            message['FavoriteCount'] = int(elem.attrib['FavoriteCount']);

        return message;
    except:
        error = '[Posts] Failed to process:'
        for key, value in elem.attrib.items():
            error += '\n' + key + ' : ' + value
        logger.error(error)
        return None;

def getMessageForUser(elem):
    try:

        #message = {}
        #for key, value in elem.attrib.items():
        #    message[key] = value

        message = {
                   'Reputation': int(elem.attrib['Reputation']),
                   'DisplayName': elem.attrib['DisplayName'],
                   'CreationDate': elem.attrib['CreationDate']}

        if 'Location' in elem.attrib:
            message['Location'] = elem.attrib['Location'];
        if 'AboutMe' in elem.attrib:
            message['AboutMe'] = elem.attrib['AboutMe'];
        if 'Age' in elem.attrib:
            message['Age'] = int(elem.attrib['Age']);
        if 'Views' in elem.attrib:
            message['Views'] = int(elem.attrib['Views']);
        if 'UpVotes' in elem.attrib:
            message['UpVotes'] = int(elem.attrib['UpVotes']);
        if 'DownVotes' in elem.attrib:
            message['DownVotes'] = int(elem.attrib['DownVotes']);

        return message;
    except:
        error = '[Users] Failed to process:'
        for key, value in elem.attrib.items():
            error += '\n' + key + ' : ' + value
        logger.error(error)
        return None;


def publishUsersToKafka(usersFilePath, address, topic, delay):
    producer = KafkaProducer(bootstrap_servers=[f'{address}:9092'],
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                             api_version=(2, 6, 0))

    logger.info('[Users] Producer started')

    data = etree.parse(usersFilePath)
    root = data.getroot()

    cnt = 0;
    try:
        for elem in root:
            registeredUser = {}
            for key, value in elem.attrib.items():
               registeredUser[key] = value
            #registeredUser = getMessageForUser(elem)
            logger.info(json.dumps(registeredUser))
            if registeredUser == None:
                continue;
            producer.send(topic, value=registeredUser)
            cnt += 1
            if (cnt % 100 == 0):
                logger.info('[Users] Sent {0} messages'.format(cnt))
            sleep(delay)
            if cnt > 10:
                break
    except KeyboardInterrupt:
        producer.close()
    except Exception as err:
        logger.error("[Users] Unexpected exception: {0}".format(str(err)))
        producer.close()
    finally:
        logger.info("[Users] Producer finished")


def publishPostsToKafka(postsFilePath, address, topic, delay):
    producer = KafkaProducer(bootstrap_servers=[f'{address}:9092'],
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                             api_version=(2, 6, 0))

    logger.info('[Posts] Producer started')

    data = etree.parse(postsFilePath)
    root = data.getroot()

    cnt = 0;
    try:
        for elem in root:
            message = getMessageForPost(elem)
            print(json.dumps(message))
            if message == None:
                continue;
            #producer.send(topic, value=message)
            cnt += 1
            if (cnt % 100 == 0):
                logger.info('[Posts] Sent {0} messages'.format(cnt))
            sleep(delay)
    except KeyboardInterrupt:
        producer.close()
    except Exception as err:
        logger.error("[Posts] Unexpected exception: {}".format(err.message))
        producer.close()



#setup logger
logger = logging.getLogger('MessageGenerator')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('messageGenerator.log')
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)

#parse input arguments
parser = argparse.ArgumentParser(description='Kafka message generator')
parser.add_argument('kafka', help='Address of one of kafka servers')
parser.add_argument('filePath', help='The path to the source file')
args = parser.parse_args()

postsFilePath = args.filePath + 'Posts.xml'
logger.info('[Posts] File path:' + postsFilePath)

usersFilePath = args.filePath + 'Users.xml'
logger.info('[Users] File path:' + usersFilePath)


t1 = threading.Thread(name='1', target=publishUsersToKafka, args=[usersFilePath, args.kafka, 'users', 0.1])
#t2 = threading.Thread(name='2', target=publishPostsToKafka, args=[postsFilePath, args.kafka, 'posts', 1])

t1.start()
#t2.start()