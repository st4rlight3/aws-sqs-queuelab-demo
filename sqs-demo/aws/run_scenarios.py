"""Run the same business worker against real SQS, for example in AWS CloudShell."""
import argparse,json,sys,time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from broker import AWSQueue
from core import QueueLab,Store


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--config',type=Path,default=ROOT/'.queuelab_aws.json')
    parser.add_argument('--run',action='store_true')
    args=parser.parse_args()
    if not args.run:
        print('Plan: send 3 fake orders to your configured SQS queue, process them, and inspect the DLQ. No queue is created or deleted. Pass --run to execute.')
        return
    config=json.loads(args.config.read_text(encoding='utf-8-sig'))
    queue=AWSQueue(config['queue_url'],config['dlq_url'],config['region'])
    store=Store(ROOT/'data'/'aws-cli'/'application.db')
    lab=QueueLab(queue,store,processing_seconds=.15)
    orders=[lab.enqueue({'customer':'AWS demo '+str(i+1),'product':product,'quantity':i+1,'behavior':behavior}) for i,(product,behavior) in enumerate([('coffee','normal'),('notebook','fail_once'),('bottle','always_fail')])]
    ids={order['order_id'] for order in orders}
    deadline=time.monotonic()+max(100,queue.visibility*(queue.max_receives+2))
    last_inspect=0
    while time.monotonic()<deadline:
        lab.process_once(delay=.15)
        if time.monotonic()-last_inspect>4:
            lab.inspect_dlq();last_inspect=time.monotonic()
        current=[row for row in store.snapshot()['orders'] if row['order_id'] in ids]
        if len(current)==3 and sum(row['status']=='completed' for row in current)==2 and sum(row['status']=='dead' for row in current)==1:
            break
    current=[row for row in store.snapshot()['orders'] if row['order_id'] in ids]
    result={'backend':'aws','region':config['region'],'queue_name':queue.urls['main'].rsplit('/',1)[-1],'orders':[{'order_id':row['order_id'],'scenario':row['behavior'],'status':row['status'],'main_deliveries':row['deliveries'],'effect_count':row['effect_count']} for row in current],'note':'Queue counts are approximate. The permanently failing message is kept in the DLQ.'}
    print(json.dumps(result,ensure_ascii=False,indent=2))
    target=ROOT/'data'/'aws-cli'/'last-run.json';target.write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf8')
    success=len(current)==3 and sum(row['status']=='completed' for row in current)==2 and sum(row['status']=='dead' for row in current)==1
    sys.exit(0 if success else 1)


if __name__=='__main__':
    main()
