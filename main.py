import boto3
from dotenv import load_dotenv
import os
from boto3.dynamodb.conditions import Key, Attr

load_dotenv()

TABLE_NAME = os.getenv('TABLE_NAME')
print('Tabela =', TABLE_NAME)

## Obtendo o recurso e conexão com o aws dynamodb 

dynamodb = boto3.resource('dynamodb', 
endpoint_url=os.getenv('ENDPOINT_URL'), 
aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'), 
aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'), 
region_name=os.getenv('REGION_NAME'))

## Criação da tabela

table = dynamodb.create_table(
    TableName= TABLE_NAME,
    KeySchema=[
        {
            'AttributeName': 'username',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'last_name',
            'KeyType': 'RANGE'
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'username',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'last_name',
            'AttributeType': 'S'
        },
    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 5,
        'WriteCapacityUnits': 5
    }
)

table = dynamodb.Table(TABLE_NAME)

## Criando um item

table.put_item(
   Item={
        'username': 'Th',
        'first_name': 'Thiago',
        'last_name': 'Rocha',
        'age': 25,
        'account_type': 'standard_user',
    }
)

## Obtendo um item

response = table.get_item(
    Key={
        'username': 'Th',
        'last_name': 'Rocha'
    }
)
item = response['Item']
print(item)

## Atualizando um item 

response = table.update_item(
    Key={
        'username': 'Th',
        'last_name': 'Rocha'
    },
    UpdateExpression='SET age = :val1',
    ExpressionAttributeValues={
        ':val1': 26
    }
)

## Consultando um item com condição - (query)

response = table.query(KeyConditionExpression=Key('username').eq('Th'))

for x in response['Items']:
    print(x)

## Deletando um item
## Para deletar um item descomentar o trecho abaixo.

# table.delete_item(
#     Key={
#         'username': 'Th',
#         'last_name': 'Rocha'
#     }
# )   