import redis
import json
import os
from time import sleep
from random import randint

if __name__ == '__main__':
    redis_host = os.getenv('REDIS_HOST', 'queue')
    r = redis.Redis(host=redis_host, port=6379, db=0)
    print('Aguardando mensagens ...')
    while True:
        subject = json.loads(r.blpop('sender')[1])
        print(subject)
        print('Mandando a mensagem:', subject['subject'])
        sleep(randint(11, 25))
        print('subject', subject['subject'], 'enviada')