from time import sleep
from datetime import datetime
import json
import argparse
import threading, time

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
        print('[Posts] Failed to process:')
        for key, value in elem.attrib.items():
            print(key, ' : ', value)
        return None;

def getMessageForUser(elem):
    try:
        message = {'Id': int(elem.attrib['Id']),
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
        print('[Users] Failed to process:')
        for key, value in elem.attrib.items():
            print(key, ' : ', value)
        return None;






def publishUsersToKafka(usersFilePath, address, topic, delay):
    producer = KafkaProducer(bootstrap_servers=[f'{address}:9092'],
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                             api_version=(2, 6, 0))

    print('[Users] Producer started')

    data = etree.parse(usersFilePath)
    root = data.getroot()

    try:
        for elem in root:
            message = getMessageForUser(elem)
            if message == None:
                continue;
            producer.send(topic, value=message)
            print(message)
            sleep(delay)
    except KeyboardInterrupt:
        producer.close()


def publishPostsToKafka(postsFilePath, address, topic, delay):
    producer = KafkaProducer(bootstrap_servers=[f'{address}:9092'],
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                             api_version=(2, 6, 0))

    print('[Posts] Producer started')

    data = etree.parse(postsFilePath)
    root = data.getroot()

    try:
        for elem in root:
            message = getMessageForPost(elem)

            if message == None:
                continue;
            producer.send(topic, value=message)
            #print(message)
            sleep(delay)
    except KeyboardInterrupt:
        producer.close()



parser = argparse.ArgumentParser(description='Kafka data generator')
parser.add_argument('kafka', help='address of one of kafka servers')
parser.add_argument('filePath', help='the path to the source file')
args = parser.parse_args()

postsFilePath = args.filePath + '/Posts.xml'
print(postsFilePath)

usersFilePath = args.filePath + '/Users.xml'
print(usersFilePath)


t1 = threading.Thread(name='1', target=publishUsersToKafka, args=[usersFilePath, args.kafka, 'users', 0.1])
t2 = threading.Thread(name='2', target=publishPostsToKafka, args=[postsFilePath, args.kafka, 'posts', 1])

t1.start()
t2.start()