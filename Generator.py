from time import sleep
import json
import argparse
import threading, time
import logging

from kafka import KafkaProducer
import xml.etree.ElementTree as etree


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
            message = {}
            for key, value in elem.attrib.items():
               message[key] = value
            print(json.dumps(message))
            logger.info(json.dumps(message))
            producer.send(topic, value=message)
            cnt += 1
            if (cnt % 1000 == 0):
                logger.info('[Posts] Sent {0} messages'.format(cnt))
            sleep(delay)
            if cnt > 10:
                break
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

t2 = threading.Thread(name='2', target=publishPostsToKafka, args=[postsFilePath, args.kafka, 'posts', 0.1])

t2.start()