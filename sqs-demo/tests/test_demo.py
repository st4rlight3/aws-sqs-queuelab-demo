import json,os,socket,subprocess,sys,tempfile,threading,time,unittest
from pathlib import Path
from urllib.request import Request,urlopen
from urllib.error import HTTPError

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from broker import LocalQueue,AWSQueue,Message
from core import QueueLab,Store,validate,parse_message

class Clock:
    def __init__(self):self.value=1000.0
    def __call__(self):return self.value
    def advance(self,value=11):self.value+=value

class QueueTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='queuelab-test-')
        self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.clock=Clock()
        self.queue=LocalQueue(self.root/'queue.db',clock=self.clock)
        self.store=Store(self.root/'app.db')
        self.lab=QueueLab(self.queue,self.store)
    def order(self,behavior='normal'):
        return self.lab.enqueue({'customer':'Khach demo','product':'coffee','quantity':2,'behavior':behavior})
    def test_01_paused_worker_keeps_order_waiting(self):
        self.order();self.assertEqual(self.queue.stats()['waiting'],1);self.assertEqual(self.store.snapshot()['effects'],0)
    def test_02_receiving_does_not_delete(self):
        self.order();message=self.queue.receive();self.assertIsNotNone(message);self.assertEqual(self.queue.stats()['inflight'],1);self.assertIsNone(self.queue.receive())
    def test_03_timeout_makes_message_visible_again(self):
        self.order();first=self.queue.receive();self.clock.advance();second=self.queue.receive();self.assertEqual(first.message_id,second.message_id);self.assertEqual(second.count,2);self.assertNotEqual(first.receipt,second.receipt)
    def test_04_old_receipt_cannot_delete_new_delivery(self):
        self.order();first=self.queue.receive();self.clock.advance();second=self.queue.receive()
        with self.assertRaises(ValueError):self.queue.delete(first)
        self.queue.delete(second);self.assertEqual(self.queue.stats()['waiting']+self.queue.stats()['inflight'],0)
    def test_05_success_commits_effect_then_deletes(self):
        order=self.order();self.lab.process_once(delay=0);self.assertTrue(self.store.completed(order['order_id']));self.assertEqual(self.queue.stats()['waiting']+self.queue.stats()['inflight'],0)
    def test_06_transient_failure_retries_successfully(self):
        self.order('fail_once');self.lab.process_once(delay=0);self.assertEqual(self.store.snapshot()['effects'],0);self.assertEqual(self.store.snapshot()['orders'][0]['status'],'retry');self.clock.advance();self.lab.process_once(delay=0);self.assertEqual(self.store.snapshot()['effects'],1)
    def test_07_permanent_failure_reaches_dlq(self):
        self.order('always_fail')
        for _ in range(3):self.assertTrue(self.lab.process_once(delay=0));self.clock.advance()
        self.assertFalse(self.lab.process_once(delay=0));self.assertEqual(self.queue.stats()['dead'],1);self.assertEqual(self.store.snapshot()['effects'],0)
    def test_08_dlq_inspection_preserves_message(self):
        order=self.order('always_fail')
        for _ in range(3):self.lab.process_once(delay=0);self.clock.advance()
        self.lab.process_once(delay=0)
        self.assertEqual(self.lab.inspect_dlq(),[order['order_id']]);self.assertEqual(self.queue.stats()['dead'],1);self.assertEqual(self.store.snapshot()['orders'][0]['status'],'dead')
    def test_09_duplicate_order_has_one_business_effect(self):
        order=self.order();self.lab.process_once(delay=0);self.lab.send_existing(order['order_id']);self.lab.process_once(delay=0)
        snapshot=self.store.snapshot();self.assertEqual(snapshot['effects'],1);self.assertEqual(snapshot['orders'][0]['deliveries'],2);self.assertTrue(any(e['kind']=='duplicate' for e in snapshot['events']))
    def test_10_delete_failure_does_not_repeat_business_effect(self):
        self.order();original=self.queue.delete
        def fail(message):raise RuntimeError('simulated network failure')
        self.queue.delete=fail;self.lab.process_once(delay=0);self.assertEqual(self.store.snapshot()['effects'],1)
        self.queue.delete=original;self.clock.advance();self.lab.process_once(delay=0);self.assertEqual(self.store.snapshot()['effects'],1);self.assertEqual(self.queue.stats()['inflight'],0)
    def test_11_worker_abandonment_allows_redelivery(self):
        self.order();thread=threading.Thread(target=lambda:self.lab.process_once(delay=10));thread.start()
        deadline=time.monotonic()+2
        while not self.lab.worker_state()['current'] and time.monotonic()<deadline:time.sleep(.005)
        self.assertIsNotNone(self.lab.worker_state()['current']);self.lab.control('crash');thread.join(2);self.assertFalse(thread.is_alive());self.assertEqual(self.store.snapshot()['effects'],0)
        self.clock.advance();self.lab.process_once(delay=0);self.assertEqual(self.store.snapshot()['effects'],1)
    def test_12_queue_and_ledger_persist_after_restart(self):
        first=self.order();self.lab.process_once(delay=0);second=self.order()
        queue=LocalQueue(self.root/'queue.db',clock=self.clock);store=Store(self.root/'app.db');lab=QueueLab(queue,store)
        self.assertTrue(store.completed(first['order_id']));self.assertEqual(queue.stats()['waiting'],1);lab.process_once(delay=0);self.assertTrue(store.completed(second['order_id']))
    def test_13_invalid_json_goes_through_retry_path(self):
        self.queue.send('not json');self.assertTrue(self.lab.process_once(delay=0));self.assertEqual(self.queue.stats()['inflight'],1);self.assertEqual(self.store.snapshot()['effects'],0)
    def test_14_conflicting_payload_same_id_is_rejected(self):
        order=self.order();changed={**order,'quantity':9};self.queue.receive();self.queue.send(json.dumps(changed));self.lab.process_once(delay=0);self.assertEqual(self.store.snapshot()['effects'],0);self.assertTrue(any(e['kind']=='invalid' for e in self.store.snapshot()['events']))
    def test_15_input_rejects_boolean_quantity(self):
        with self.assertRaises(ValueError):validate({'customer':'Test','product':'coffee','quantity':True})
    def test_16_input_rejects_unknown_product(self):
        with self.assertRaises(ValueError):validate({'customer':'Test','product':'invalid','quantity':1})
    def test_17_input_rejects_blank_customer(self):
        with self.assertRaises(ValueError):validate({'customer':'  ','product':'coffee','quantity':1})
    def test_18_input_rejects_unknown_behavior(self):
        with self.assertRaises(ValueError):validate({'customer':'Test','product':'coffee','quantity':1,'behavior':'other'})
    def test_19_worker_step_is_rejected_during_auto_mode(self):
        self.lab.control('start')
        with self.assertRaises(ValueError):self.lab.control('step')
        self.lab.control('pause');self.assertFalse(self.lab.worker_state()['active'])
    def test_20_order_body_is_validated_before_processing(self):
        with self.assertRaises(ValueError):parse_message(json.dumps({'schema':1,'order_id':'wrong','customer':'Test','product':'coffee','quantity':1,'created_at':1000}))

class FakeAWS:
    def __init__(self):self.calls=[];self.messages=[]
    def get_queue_attributes(self,**kwargs):
        self.calls.append(('get_queue_attributes',kwargs))
        if kwargs['AttributeNames']==['QueueArn']:
            return {'Attributes':{'QueueArn':'arn:aws:sqs:ap-southeast-2:000000000000:dlq'}}
        if 'RedrivePolicy' in kwargs['AttributeNames']:
            return {'Attributes':{'QueueArn':'arn:aws:sqs:ap-southeast-2:000000000000:main','VisibilityTimeout':'10','RedrivePolicy':json.dumps({'deadLetterTargetArn':'arn:aws:sqs:ap-southeast-2:000000000000:dlq','maxReceiveCount':'3'})}}
        return {'Attributes':{'ApproximateNumberOfMessages':'2','ApproximateNumberOfMessagesNotVisible':'1'}}
    def receive_message(self,**kwargs):self.calls.append(('receive_message',kwargs));return {'Messages':self.messages}
    def send_message(self,**kwargs):self.calls.append(('send_message',kwargs));return {'MessageId':'aws-message-1'}
    def delete_message(self,**kwargs):self.calls.append(('delete_message',kwargs));return {}
    def change_message_visibility(self,**kwargs):self.calls.append(('change_message_visibility',kwargs));return {}

class AWSAdapterTests(unittest.TestCase):
    def setUp(self):self.fake=FakeAWS();self.queue=AWSQueue('https://example.test/main','https://example.test/dlq','ap-southeast-2',client=self.fake)
    def test_21_aws_reads_real_queue_redrive_configuration(self):self.assertEqual(self.queue.max_receives,3);self.assertEqual(self.queue.visibility,10)
    def test_22_aws_receive_uses_system_attributes_and_long_polling(self):
        self.fake.messages=[{'MessageId':'m1','Body':'{}','ReceiptHandle':'receipt-new','Attributes':{'ApproximateReceiveCount':'2'}}]
        message=self.queue.receive();call=self.fake.calls[-1][1];self.assertEqual(message.count,2);self.assertEqual(call['WaitTimeSeconds'],2);self.assertIn('ApproximateReceiveCount',call['MessageSystemAttributeNames'])
    def test_23_aws_delete_uses_receipt_handle(self):self.queue.delete(Message('m1','{}','receipt-1',1));self.assertEqual(self.fake.calls[-1][1],{'QueueUrl':'https://example.test/main','ReceiptHandle':'receipt-1'})
    def test_24_aws_release_does_not_delete_dlq_message(self):self.queue.release(Message('m1','{}','r1',1,'dlq'));self.assertEqual(self.fake.calls[-1][0],'change_message_visibility');self.assertEqual(self.fake.calls[-1][1]['VisibilityTimeout'],0)
    def test_25_aws_stats_are_marked_approximate(self):self.assertTrue(self.queue.stats()['approximate']);self.assertEqual(self.queue.stats()['waiting'],2)
    def test_26_aws_empty_receive_is_not_treated_as_failure(self):self.assertIsNone(self.queue.receive())

class HTTPTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp=tempfile.TemporaryDirectory(prefix='queuelab-http-')
        with socket.socket() as sock:sock.bind(('127.0.0.1',0));cls.port=sock.getsockname()[1]
        cls.log=open(Path(cls.temp.name)/'server.log','w',encoding='utf8')
        cls.proc=subprocess.Popen([sys.executable,str(ROOT/'app.py'),'--port',str(cls.port),'--data-dir',cls.temp.name],stdout=cls.log,stderr=cls.log)
        for _ in range(60):
            try:
                if cls.request('/health')[0]==200:break
            except Exception:time.sleep(.05)
        else:raise RuntimeError('HTTP server failed to start')
    @classmethod
    def tearDownClass(cls):cls.proc.terminate();cls.proc.wait(timeout=8);cls.log.close();cls.temp.cleanup()
    @classmethod
    def request(cls,path,body=None,header=True,raw=False):
        headers={}
        if header:headers['X-QueueLab-Request']='1'
        if body is not None:headers['Content-Type']='application/json'
        req=Request(f'http://127.0.0.1:{cls.port}'+path,data=json.dumps(body).encode() if body is not None else None,headers=headers)
        try:response=urlopen(req,timeout=5)
        except HTTPError as exc:response=exc
        data=response.read();return response.status,data if raw else json.loads(data)
    def test_27_health_reports_local_mode(self):self.assertEqual(self.request('/health')[1]['backend'],'local')
    def test_28_http_post_without_custom_header_is_rejected(self):self.assertEqual(self.request('/api/orders',{},header=False)[0],403)
    def test_29_http_rejects_nonobject_json(self):self.assertEqual(self.request('/api/orders',[])[0],400)
    def test_30_http_enqueue_and_state_work(self):
        status,order=self.request('/api/orders',{'customer':'HTTP Demo','product':'bottle','quantity':1,'behavior':'normal'});self.assertEqual(status,201);self.assertTrue(any(o['order_id']==order['order_id'] for o in self.request('/api/state')[1]['orders']))
    def test_31_unknown_path_is_not_served_as_static_file(self):self.assertEqual(self.request('/../core.py',raw=True)[0],404)
    def test_32_web_interface_is_served(self):self.assertIn('QueueLab'.encode(),self.request('/',raw=True)[1])

if __name__=='__main__':
    suite=unittest.defaultTestLoader.loadTestsFromModule(sys.modules[__name__])
    names=[test.id() for group in suite for test in group]
    result=unittest.TextTestRunner(verbosity=2).run(suite)
    evidence={'tests':result.testsRun,'passed':result.testsRun-len(result.failures)-len(result.errors),'failures':len(result.failures),'errors':len(result.errors),'scope':'Local queue model, HTTP API and AWS SDK adapter with fake client; no live AWS calls','cases':names}
    Path(__file__).with_name('verification.json').write_text(json.dumps(evidence,ensure_ascii=False,indent=2),encoding='utf8')
    sys.exit(not result.wasSuccessful())
