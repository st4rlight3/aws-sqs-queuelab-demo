"""Create two private Standard queues for the demo using the existing AWS identity."""
import argparse,json,re,time
from pathlib import Path


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--region',default='ap-southeast-2')
    parser.add_argument('--prefix',default='queuelab-'+time.strftime('%Y%m%d%H%M%S'))
    parser.add_argument('--create',action='store_true')
    args=parser.parse_args()
    if not re.fullmatch(r'queuelab-[a-zA-Z0-9-]{1,50}',args.prefix):
        parser.error('Prefix must start with queuelab- and contain only letters, digits and hyphens')
    main_name=args.prefix+'-orders'
    dlq_name=args.prefix+'-dlq'
    print(json.dumps({'region':args.region,'queues':[main_name,dlq_name],'type':'Standard','visibility_seconds':10,'max_receive_count':3,'retention_seconds':[86400,172800],'encryption':'SSE-SQS'},indent=2))
    if not args.create:
        print('Plan only. Pass --create to create these AWS resources using your current identity.')
        return
    import boto3
    from botocore.exceptions import ClientError
    client=boto3.client('sqs',region_name=args.region)
    for name in [main_name,dlq_name]:
        try:
            client.get_queue_url(QueueName=name)
        except ClientError as exc:
            if exc.response['Error']['Code'] not in ('AWS.SimpleQueueService.NonExistentQueue','QueueDoesNotExist'):
                raise
        else:
            raise RuntimeError('Queue already exists. Choose a new prefix; existing queues are not modified.')
    tags={'Project':'QueueLabDemo','Purpose':'Classroom','ManagedBy':'QueueLabSetup'}
    dlq_url=client.create_queue(QueueName=dlq_name,Attributes={'VisibilityTimeout':'10','MessageRetentionPeriod':'172800','SqsManagedSseEnabled':'true'},tags=tags)['QueueUrl']
    dlq_arn=client.get_queue_attributes(QueueUrl=dlq_url,AttributeNames=['QueueArn'])['Attributes']['QueueArn']
    queue_url=client.create_queue(QueueName=main_name,Attributes={'VisibilityTimeout':'10','MessageRetentionPeriod':'86400','ReceiveMessageWaitTimeSeconds':'2','SqsManagedSseEnabled':'true','RedrivePolicy':json.dumps({'deadLetterTargetArn':dlq_arn,'maxReceiveCount':'3'})},tags=tags)['QueueUrl']
    main_arn=client.get_queue_attributes(QueueUrl=queue_url,AttributeNames=['QueueArn'])['Attributes']['QueueArn']
    client.set_queue_attributes(QueueUrl=dlq_url,Attributes={'RedriveAllowPolicy':json.dumps({'redrivePermission':'byQueue','sourceQueueArns':[main_arn]})})
    config={'region':args.region,'queue_url':queue_url,'dlq_url':dlq_url,'prefix':args.prefix}
    destination=Path(__file__).resolve().parents[1]/'.queuelab_aws.json'
    destination.write_text(json.dumps(config,indent=2),encoding='utf8')
    print('Created both queues. Configuration saved to '+destination.name)
    print('Run: python3 aws/run_scenarios.py --run')


if __name__=='__main__':
    main()
