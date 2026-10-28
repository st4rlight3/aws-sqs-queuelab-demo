"""Business state, observable events and a worker with idempotent local effects."""
import json
import math
import re
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

PRODUCTS = {'coffee':'Cà phê rang xay','notebook':'Sổ tay học tập','bottle':'Bình giữ nhiệt'}
BEHAVIORS = ('normal','fail_once','always_fail')


def error_text(exc):
    if isinstance(exc,ValueError):
        return str(exc)
    response = getattr(exc,'response',{})
    return response.get('Error',{}).get('Code',type(exc).__name__)


def validate(values):
    if not isinstance(values,dict):
        raise ValueError('Dữ liệu phải là đối tượng JSON')
    customer = values.get('customer','')
    if not isinstance(customer,str) or not 1 <= len(customer.strip()) <= 60:
        raise ValueError('Tên khách minh họa cần từ 1 đến 60 ký tự')
    product = values.get('product')
    quantity = values.get('quantity')
    behavior = values.get('behavior','normal')
    if not isinstance(product,str) or product not in PRODUCTS:
        raise ValueError('Sản phẩm không hợp lệ')
    if type(quantity) is not int or not 1 <= quantity <= 20:
        raise ValueError('Số lượng phải là số nguyên từ 1 đến 20')
    if not isinstance(behavior,str) or behavior not in BEHAVIORS:
        raise ValueError('Kịch bản xử lý không hợp lệ')
    return {'customer':customer.strip(),'product':product,'quantity':quantity,'behavior':behavior}


def parse_message(body):
    try:
        payload = json.loads(body)
        business = validate(payload)
        if type(payload.get('schema')) is not int or payload.get('schema') != 1 or not isinstance(payload.get('order_id'),str) or not re.fullmatch(r'QL-[a-f0-9]{10}',payload['order_id']):
            raise ValueError('Thông điệp không đúng schema đơn hàng')
        created = payload.get('created_at')
        if type(created) not in (int,float) or not math.isfinite(created):
            raise ValueError('Thời điểm tạo không hợp lệ')
        return {**business,'schema':1,'order_id':payload['order_id'],'created_at':created}
    except (json.JSONDecodeError,TypeError) as exc:
        raise ValueError('Không đọc được thông điệp JSON') from exc


class Store:
    def __init__(self,path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True,exist_ok=True)
        self.lock = threading.RLock()
        with self.db() as db:
            db.execute('PRAGMA journal_mode=WAL')
            db.execute('CREATE TABLE IF NOT EXISTS orders (order_id TEXT PRIMARY KEY,payload TEXT NOT NULL,status TEXT NOT NULL,deliveries INTEGER NOT NULL DEFAULT 0,last_count INTEGER NOT NULL DEFAULT 0,last_error TEXT NOT NULL DEFAULT \'\',created_at REAL NOT NULL)')
            db.execute('CREATE TABLE IF NOT EXISTS effects (order_id TEXT PRIMARY KEY,processed_at REAL NOT NULL)')
            db.execute('CREATE TABLE IF NOT EXISTS events (seq INTEGER PRIMARY KEY AUTOINCREMENT,at REAL NOT NULL,kind TEXT NOT NULL,order_id TEXT,message TEXT NOT NULL)')

    @contextmanager
    def db(self):
        with self.lock:
            db = sqlite3.connect(self.path,timeout=10)
            db.row_factory = sqlite3.Row
            try:
                with db:
                    yield db
            finally:
                db.close()

    def ensure(self,payload,status='queued'):
        with self.db() as db:
            old = db.execute('SELECT payload FROM orders WHERE order_id=?',(payload['order_id'],)).fetchone()
            if old:
                a,b = validate(json.loads(old['payload'])),validate(payload)
                if a != b:
                    raise ValueError('Thông điệp trùng order_id nhưng khác nội dung nghiệp vụ')
                return
            db.execute('INSERT INTO orders(order_id,payload,status,created_at) VALUES(?,?,?,?)',(payload['order_id'],json.dumps(payload,ensure_ascii=False),status,payload['created_at']))

    def payload(self,order_id):
        with self.db() as db:
            row = db.execute('SELECT payload FROM orders WHERE order_id=?',(order_id,)).fetchone()
        if row is None:
            raise ValueError('Không tìm thấy đơn hàng')
        return json.loads(row['payload'])

    def event(self,kind,order_id,message):
        with self.db() as db:
            db.execute('INSERT INTO events(at,kind,order_id,message) VALUES(?,?,?,?)',(time.time(),kind,order_id,message))

    def status(self,order_id,status,error=''):
        with self.db() as db:
            db.execute("UPDATE orders SET status=?,last_error=? WHERE order_id=? AND status!='completed'",(status,error,order_id))

    def received(self,payload,count):
        self.ensure(payload)
        with self.db() as db:
            db.execute('UPDATE orders SET deliveries=deliveries+1,last_count=? WHERE order_id=?',(count,payload['order_id']))
        self.status(payload['order_id'],'processing')

    def completed(self,order_id):
        with self.db() as db:
            return db.execute('SELECT 1 FROM effects WHERE order_id=?',(order_id,)).fetchone() is not None

    def finish_once(self,order_id):
        with self.db() as db:
            result = db.execute('INSERT OR IGNORE INTO effects(order_id,processed_at) VALUES(?,?)',(order_id,time.time()))
            db.execute("UPDATE orders SET status='completed',last_error='' WHERE order_id=?",(order_id,))
            return result.rowcount == 1

    def snapshot(self):
        with self.db() as db:
            rows = db.execute('SELECT * FROM orders ORDER BY created_at DESC').fetchall()
            effects = db.execute('SELECT COUNT(*) FROM effects').fetchone()[0]
            events = [dict(row) for row in db.execute('SELECT * FROM events ORDER BY seq DESC LIMIT 45')]
        orders = []
        for row in rows:
            value = dict(row)
            payload = json.loads(value.pop('payload'))
            value.update(payload)
            value['effect_count'] = int(value['status']=='completed')
            orders.append(value)
        return {'orders':orders,'effects':effects,'events':events}


class QueueLab:
    def __init__(self,backend,store,processing_seconds=4):
        self.backend = backend
        self.store = store
        self.processing_seconds = processing_seconds
        self.lock = threading.RLock()
        self.active = False
        self.single = False
        self.busy = False
        self.current = None
        self.started_at = None
        self.abort = threading.Event()
        self.wake = threading.Event()
        self.closed = threading.Event()
        self.thread = threading.Thread(target=self.loop,daemon=True)

    def start_thread(self):
        self.thread.start()

    def enqueue(self,values):
        data = {**validate(values),'schema':1,'order_id':'QL-'+uuid.uuid4().hex[:10],'created_at':time.time()}
        self.store.ensure(data,status='sending')
        self.send_existing(data['order_id'])
        return data

    def send_existing(self,order_id):
        payload = self.store.payload(order_id)
        try:
            message_id = self.backend.send(json.dumps(payload,ensure_ascii=False))
            self.store.status(order_id,'queued')
            self.store.event('sent',order_id,'SendMessage thành công · '+message_id[:12])
            return message_id
        except Exception as exc:
            self.store.status(order_id,'send_error',error_text(exc))
            self.store.event('send_error',order_id,error_text(exc))
            raise

    def control(self,action):
        with self.lock:
            if action == 'start':
                self.active = True
                self.wake.set()
            elif action == 'pause':
                self.active = False
            elif action == 'step':
                if self.busy or self.active or self.single:
                    raise ValueError('Worker đang bận hoặc đang chạy tự động')
                self.single = True
                self.wake.set()
            elif action == 'crash':
                if not self.current:
                    raise ValueError('Worker chưa giữ thông điệp nào để giả lập sự cố')
                self.active = False
                self.abort.set()
            else:
                raise ValueError('Thao tác worker không hợp lệ')
        self.store.event('worker',None,{'start':'Bật worker tự động','pause':'Dừng nhận đơn mới; hoàn tất đơn đang xử lý','step':'Nhận và xử lý tối đa một thông điệp','crash':'Giả lập bỏ dở; không gọi DeleteMessage'}[action])

    def worker_state(self):
        with self.lock:
            return {'active':self.active,'busy':self.busy,'current':self.current,'started_at':self.started_at,'processing_seconds':self.processing_seconds}

    def loop(self):
        while not self.closed.is_set():
            self.wake.wait(.25)
            with self.lock:
                if not self.active and not self.single:
                    self.wake.clear()
                    continue
                was_single = self.single
                self.single = False
                self.busy = True
            try:
                found = self.process_once()
                if was_single and not found:
                    self.store.event('empty',None,'ReceiveMessage chưa trả về thông điệp')
            except Exception as exc:
                self.store.event('error',None,error_text(exc))
                with self.lock:
                    self.active = False
            finally:
                with self.lock:
                    self.busy = False
                    self.current = None
                    self.started_at = None
            self.closed.wait(.3)

    def process_once(self,delay=None):
        self.abort.clear()
        message = self.backend.receive()
        if message is None:
            return False
        try:
            payload = parse_message(message.body)
            order_id = payload['order_id']
            self.store.received(payload,message.count)
        except ValueError as exc:
            self.store.event('invalid',None,error_text(exc)+' · để SQS thử lại hoặc chuyển DLQ')
            return True
        self.store.event('received',order_id,f'ReceiveMessage · lượt nhận của thông điệp {message.count}')
        with self.lock:
            self.current = order_id
            self.started_at = time.time()
        try:
            if self.store.completed(order_id):
                self.store.event('duplicate',order_id,'Đã có kết quả nghiệp vụ; bỏ qua xử lý trùng')
                self.backend.delete(message)
                self.store.event('deleted',order_id,'DeleteMessage sau khi xác nhận kết quả đã tồn tại')
                return True
            if self.abort.wait(self.processing_seconds if delay is None else delay):
                self.store.status(order_id,'retry','Worker bỏ dở; chờ hết visibility timeout')
                self.store.event('abandoned',order_id,'Không xóa thông điệp; sẽ được nhận lại sau timeout')
                return True
            failure = payload['behavior']=='always_fail' or (payload['behavior']=='fail_once' and message.count==1)
            if failure:
                self.store.status(order_id,'retry','Lỗi được chủ động tạo cho bài demo')
                self.store.event('failed',order_id,'Xử lý lỗi · không gọi DeleteMessage')
                return True
            created = self.store.finish_once(order_id)
            self.store.event('completed' if created else 'duplicate',order_id,'Ghi kết quả nghiệp vụ đúng một lần cho order_id này')
            try:
                self.backend.delete(message)
                self.store.event('deleted',order_id,'DeleteMessage thành công')
            except Exception as exc:
                self.store.event('ack_error',order_id,'Đã ghi nghiệp vụ nhưng chưa xác nhận xóa · '+error_text(exc))
            return True
        finally:
            with self.lock:
                self.current = None
                self.started_at = None

    def inspect_dlq(self):
        held = []
        seen = []
        try:
            for _ in range(10):
                message = self.backend.receive('dlq',wait=0)
                if message is None:
                    break
                held.append(message)
                try:
                    payload = parse_message(message.body)
                    self.store.ensure(payload)
                    order_id = payload['order_id']
                    self.store.status(order_id,'dead','Đã xác nhận thông điệp ở DLQ')
                    self.store.event('dead',order_id,'Nhận kiểm tra từ DLQ; giữ lại thông điệp để điều tra')
                    seen.append(order_id)
                except ValueError as exc:
                    self.store.event('invalid_dlq',None,error_text(exc))
        finally:
            for message in held:
                self.backend.release(message)
        return list(dict.fromkeys(seen))

    def state(self):
        return {**self.store.snapshot(),'queue':self.backend.stats(),'worker':self.worker_state(),'server_time':time.time()}

    def close(self):
        self.closed.set()
        self.abort.set()
        self.wake.set()
        if self.thread.is_alive():
            self.thread.join(timeout=6)
