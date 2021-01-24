from time import sleep
import json
import argparse
import threading, time
import logging
from kafka import KafkaProducer
import xml.etree.ElementTree as etree
from datetime import datetime



def publishPostsToKafka(postsFilePath, address, topic, delay):
    producer = KafkaProducer(bootstrap_servers=[f'{address}:9092'],
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                             api_version=(2, 6, 0))

    logger.info('[Posts] Producer started')

    data = etree.parse(postsFilePath)
    root = data.getroot()

    #lastCreationDate = datetime.strptime('2013-07-19T06:17:13.607', '%Y-%m-%dT%H:%M:%S.%f')
    cnt = 0;
    try:
        for elem in root:
            message = {}
            for key, value in elem.attrib.items():
               message[key] = value

            #print(json.dumps(message))
            #logger.info(json.dumps(message))

            creationDate = datetime.strptime(message["CreationDate"], '%Y-%m-%dT%H:%M:%S.%f')

            #if creationDate <= lastCreationDate:
            #    continue;

            producer.send(topic, value=message)
            cnt += 1
            if (cnt % 1000 == 0):
                logger.info('[Posts] Sent {0} messages'.format(cnt))
                logger.info(json.dumps(message))

            #if cnt > 30:
            #   break;
            sleep(delay)
        logger.info('[Posts] Producer completed')
    except KeyboardInterrupt:
        producer.close()
    except Exception as err:
        logger.error("[Posts] Unexpected exception: {}".format(err))
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
parser.add_argument('kafkaAddress', help='Address of one of kafka servers')
parser.add_argument('filePath', help='The path to the source file')
parser.add_argument('topicName', help='Kafka topic')
args = parser.parse_args()

postsFilePath = args.filePath
logger.info('[Posts] File path: {0}, Kafka address: {1}, Topic name: {2}'.format(postsFilePath, args.kafkaAddress, args.topicName))

t2 = threading.Thread(name='2', target=publishPostsToKafka, args=[postsFilePath, args.kafkaAddress, args.topicName, 0.001])

t2.start()