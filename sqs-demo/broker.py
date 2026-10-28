"""Queue backends used by QueueLab. The local backend models a documented subset of SQS."""
import json
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Message:
    message_id: str
    body: str
    receipt: str
    count: int
    queue: str = 'main'


class LocalQueue:
    mode = 'local'
    region = None

    def __init__(self, path, visibility=10, max_receives=3, clock=time.time):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.visibility = visibility
        self.max_receives = max_receives
        self.clock = clock
        self.lock = threading.RLock()
        with self.db() as db:
            db.execute('CREATE TABLE IF NOT EXISTS messages (seq INTEGER PRIMARY KEY AUTOINCREMENT, message_id TEXT UNIQUE, body TEXT NOT NULL, queue TEXT NOT NULL, visible_at REAL NOT NULL, receives INTEGER NOT NULL DEFAULT 0, receipt TEXT)')

    @contextmanager
    def db(self):
        with self.lock:
            db = sqlite3.connect(self.path, timeout=10)
            db.row_factory = sqlite3.Row
            try:
                with db:
                    yield db
            finally:
                db.close()

    def send(self, body):
        message_id = uuid.uuid4().hex
        with self.db() as db:
            db.execute('INSERT INTO messages(message_id,body,queue,visible_at) VALUES(?,?,?,?)', (message_id, body, 'main', self.clock()))
        return message_id

    def receive(self, queue='main', wait=0):
        now = self.clock()
        with self.db() as db:
            candidates = db.execute('SELECT * FROM messages WHERE queue=? AND visible_at<=? ORDER BY seq', (queue, now)).fetchall()
            for row in candidates:
                if queue == 'main' and row['receives'] >= self.max_receives:
                    db.execute("UPDATE messages SET queue='dlq',visible_at=?,receives=0,receipt=NULL WHERE seq=?", (now, row['seq']))
                    continue
                receipt = uuid.uuid4().hex
                count = row['receives']+1
                db.execute('UPDATE messages SET receives=?,receipt=?,visible_at=? WHERE seq=?', (count,receipt,now+self.visibility,row['seq']))
                return Message(row['message_id'],row['body'],receipt,count,queue)
        return None

    def delete(self, message):
        with self.db() as db:
            result = db.execute('DELETE FROM messages WHERE queue=? AND receipt=?', (message.queue,message.receipt))
            if result.rowcount != 1:
                raise ValueError('Receipt handle đã cũ hoặc không tồn tại')

    def release(self, message):
        with self.db() as db:
            result = db.execute('UPDATE messages SET visible_at=? WHERE queue=? AND receipt=?', (self.clock(),message.queue,message.receipt))
            if result.rowcount != 1:
                raise ValueError('Receipt handle đã cũ hoặc không tồn tại')

    def stats(self):
        now = self.clock()
        with self.db() as db:
            waiting = db.execute("SELECT COUNT(*) FROM messages WHERE queue='main' AND visible_at<=?", (now,)).fetchone()[0]
            inflight = db.execute("SELECT COUNT(*) FROM messages WHERE queue='main' AND visible_at>?", (now,)).fetchone()[0]
            dead = db.execute("SELECT COUNT(*) FROM messages WHERE queue='dlq' AND visible_at<=?", (now,)).fetchone()[0]
        return {'waiting':waiting,'inflight':inflight,'dead':dead,'approximate':False,'queue_name':'queuelab-local-orders','dlq_name':'queuelab-local-dlq','visibility':self.visibility,'max_receives':self.max_receives,'region':None,'mode':'local'}


class AWSQueue:
    mode = 'aws'

    def __init__(self, queue_url, dlq_url, region, client=None):
        if not queue_url or not dlq_url or not region:
            raise ValueError('Cần queue_url, dlq_url và region trong cấu hình AWS')
        if client is None:
            import boto3
            from botocore.config import Config
            client = boto3.client('sqs',region_name=region,config=Config(connect_timeout=5,read_timeout=25,retries={'max_attempts':2,'mode':'standard'}))
        self.client = client
        self.urls = {'main':queue_url,'dlq':dlq_url}
        self.region = region
        attrs = self.client.get_queue_attributes(QueueUrl=queue_url,AttributeNames=['QueueArn','VisibilityTimeout','RedrivePolicy'])['Attributes']
        dlq_attrs = self.client.get_queue_attributes(QueueUrl=dlq_url,AttributeNames=['QueueArn'])['Attributes']
        policy = json.loads(attrs.get('RedrivePolicy','{}'))
        if policy.get('deadLetterTargetArn') != dlq_attrs['QueueArn']:
            raise ValueError('Queue chính chưa trỏ đúng DLQ trong RedrivePolicy')
        self.visibility = int(attrs['VisibilityTimeout'])
        self.max_receives = int(policy['maxReceiveCount'])

    def send(self, body):
        return self.client.send_message(QueueUrl=self.urls['main'],MessageBody=body)['MessageId']

    def receive(self, queue='main', wait=2):
        result = self.client.receive_message(QueueUrl=self.urls[queue],MaxNumberOfMessages=1,WaitTimeSeconds=wait,MessageSystemAttributeNames=['ApproximateReceiveCount','SentTimestamp'])
        messages = result.get('Messages',[])
        if not messages:
            return None
        value = messages[0]
        return Message(value['MessageId'],value['Body'],value['ReceiptHandle'],int(value['Attributes'].get('ApproximateReceiveCount',1)),queue)

    def delete(self, message):
        self.client.delete_message(QueueUrl=self.urls[message.queue],ReceiptHandle=message.receipt)

    def release(self, message):
        self.client.change_message_visibility(QueueUrl=self.urls[message.queue],ReceiptHandle=message.receipt,VisibilityTimeout=0)

    def stats(self):
        main = self.client.get_queue_attributes(QueueUrl=self.urls['main'],AttributeNames=['ApproximateNumberOfMessages','ApproximateNumberOfMessagesNotVisible'])['Attributes']
        dlq = self.client.get_queue_attributes(QueueUrl=self.urls['dlq'],AttributeNames=['ApproximateNumberOfMessages'])['Attributes']
        return {'waiting':int(main.get('ApproximateNumberOfMessages',0)),'inflight':int(main.get('ApproximateNumberOfMessagesNotVisible',0)),'dead':int(dlq.get('ApproximateNumberOfMessages',0)),'approximate':True,'queue_name':self.urls['main'].rsplit('/',1)[-1],'dlq_name':self.urls['dlq'].rsplit('/',1)[-1],'visibility':self.visibility,'max_receives':self.max_receives,'region':self.region,'mode':'aws'}
