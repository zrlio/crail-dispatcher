import boto3
import botocore
import argparse

def invoke_lambda(args):
  client = boto3.client('lambda')
  payload = ""

  response = client.invoke(FunctionName=args.lambdaName,
                           InvocationType='Event', Payload=payload)

  if response['StatusCode'] != 202:
    print('Error in invoking Lambda')


def get_args():
  parser = argparse.ArgumentParser()
  parser.add_argument('--function', '-f', type=str, required=True,
            dest='lambdaName',
            help='Which lambda function to use')
  return parser.parse_args()

def main(args):
  print('Argument list: {}'.format(args))
  invoke_lambda(args)
  

if __name__ == '__main__':
  main(get_args())
