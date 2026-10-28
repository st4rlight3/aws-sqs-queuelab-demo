"""Write queue connection information only; never writes AWS access keys."""
import argparse,json
from pathlib import Path
from urllib.parse import urlsplit

parser=argparse.ArgumentParser()
parser.add_argument('--region',required=True)
parser.add_argument('--queue-url',required=True)
parser.add_argument('--dlq-url',required=True)
args=parser.parse_args()
for value in (args.queue_url,args.dlq_url):
    parsed=urlsplit(value)
    if parsed.scheme!='https' or not parsed.hostname or not parsed.hostname.endswith('.amazonaws.com'):
        parser.error('Use the HTTPS queue URL copied from your AWS SQS Console')
config={'region':args.region,'queue_url':args.queue_url,'dlq_url':args.dlq_url}
path=Path(__file__).resolve().parents[1]/'.queuelab_aws.json'
path.write_text(json.dumps(config,indent=2),encoding='utf8')
print('Queue connection information written to '+path.name+'. No AWS credentials were stored.')
