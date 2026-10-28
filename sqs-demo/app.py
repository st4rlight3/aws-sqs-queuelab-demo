"""QueueLab web application. Local mode uses only the Python standard library."""
import argparse
import json
import os
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit
from broker import LocalQueue,AWSQueue
from core import QueueLab,Store,error_text

ROOT=Path(__file__).resolve().parent


class Handler(BaseHTTPRequestHandler):
    server_version='QueueLab/1.0'

    def respond(self,status,body,mime='application/json; charset=utf-8'):
        if isinstance(body,(dict,list)):
            body=json.dumps(body,ensure_ascii=False).encode('utf8')
        elif isinstance(body,str):
            body=body.encode('utf8')
        self.send_response(status)
        self.send_header('Content-Type',mime)
        self.send_header('Content-Length',str(len(body)))
        self.send_header('Cache-Control','no-store')
        self.send_header('X-Content-Type-Options','nosniff')
        self.send_header('Referrer-Policy','same-origin')
        self.send_header('Content-Security-Policy',"default-src 'self'; style-src 'self'; script-src 'self'; img-src 'self' data:; object-src 'none'; base-uri 'none'")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path=urlsplit(self.path).path
        static={'/':('index.html','text/html; charset=utf-8'),'/style.css':('style.css','text/css; charset=utf-8'),'/app.js':('app.js','text/javascript; charset=utf-8')}
        if path in static:
            file,mime=static[path]
            return self.respond(200,(ROOT/'web'/file).read_bytes(),mime)
        if path=='/health':
            return self.respond(200,{'ok':True,'backend':self.server.lab.backend.mode})
        if path=='/api/state':
            try:
                return self.respond(200,self.server.lab.state())
            except Exception as exc:
                return self.respond(503,{'error':error_text(exc)})
        return self.respond(404,{'error':'Không tìm thấy đường dẫn'})

    def do_POST(self):
        if self.headers.get('X-QueueLab-Request')!='1':
            return self.respond(403,{'error':'Thiếu xác nhận yêu cầu từ giao diện'})
        try:
            length=int(self.headers.get('Content-Length','0'))
            if not 0 < length <= 8192 or self.headers.get_content_type()!='application/json':
                raise ValueError('Yêu cầu phải là JSON có kích thước hợp lệ')
            body=json.loads(self.rfile.read(length).decode('utf8'))
            if not isinstance(body,dict):
                raise ValueError('Dữ liệu phải là đối tượng JSON')
            path=urlsplit(self.path).path
            if path=='/api/orders':
                return self.respond(201,self.server.lab.enqueue(body))
            if path=='/api/resend':
                order_id=body.get('order_id')
                if not isinstance(order_id,str):
                    raise ValueError('Thiếu order_id')
                return self.respond(200,{'message_id':self.server.lab.send_existing(order_id)})
            if path=='/api/worker':
                self.server.lab.control(body.get('action'))
                return self.respond(200,{'ok':True})
            if path=='/api/dlq/inspect':
                return self.respond(200,{'orders':self.server.lab.inspect_dlq()})
            return self.respond(404,{'error':'Không tìm thấy đường dẫn'})
        except (ValueError,UnicodeError) as exc:
            return self.respond(400,{'error':error_text(exc)})
        except Exception as exc:
            return self.respond(503,{'error':error_text(exc)})


def main():
    parser=argparse.ArgumentParser(description='QueueLab Amazon SQS demo')
    parser.add_argument('--backend',choices=['local','aws'],default='local')
    parser.add_argument('--config',type=Path,default=ROOT/'.queuelab_aws.json')
    parser.add_argument('--data-dir',type=Path)
    parser.add_argument('--host',default='127.0.0.1')
    parser.add_argument('--port',type=int,default=8081)
    parser.add_argument('--open-browser',action='store_true')
    args=parser.parse_args()
    data_dir=args.data_dir or ROOT/'data'/args.backend
    if args.backend=='local':
        backend=LocalQueue(data_dir/'broker.db')
    else:
        try:
            config=json.loads(args.config.read_text(encoding='utf-8-sig'))
            backend=AWSQueue(config['queue_url'],config['dlq_url'],config['region'])
        except Exception as exc:
            parser.error('Không khởi tạo được SQS: '+error_text(exc))
    lab=QueueLab(backend,Store(data_dir/'application.db'))
    server=ThreadingHTTPServer((args.host,args.port),Handler)
    server.lab=lab
    lab.start_thread()
    print(f'QueueLab: http://{args.host}:{args.port} | backend={args.backend}',flush=True)
    if args.backend=='local':
        print('Local teaching model. This mode does not connect to Amazon SQS.',flush=True)
    else:
        print('Using real Amazon SQS through the AWS SDK credential provider chain.',flush=True)
    if args.open_browser:
        threading.Timer(.8,lambda:webbrowser.open(f'http://127.0.0.1:{args.port}')).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        lab.close()
        server.server_close()


if __name__=='__main__':
    main()
