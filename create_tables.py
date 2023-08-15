import boto3

def create_table():
    dynamodb = boto3.resource('dynamodb',  aws_access_key_id="anything", aws_secret_access_key="anything", 
                            region_name='us-east-1', endpoint_url="http://localhost:8000")

    quiz_table = dynamodb.create_table(
        TableName='Quizzes',
        KeySchema=[
            {
                'AttributeName': 'quiz_id',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'question_number',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'quiz_id',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'question_number',
                'AttributeType': 'N'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )

    unit_table = dynamodb.create_table(
        TableName='Units',
        KeySchema=[
            {
                'AttributeName': 'unit_id',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'unit_number',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'unit_id',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'unit_number',
                'AttributeType': 'N'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )

    lesson_table = dynamodb.create_table(
        TableName='Lessons',
        KeySchema=[
            {
                'AttributeName': 'lesson_id',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'lesson_number',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'lesson_id',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'lesson_number',
                'AttributeType': 'N'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )

    interactive_table = dynamodb.create_table(
        TableName='Interactives',
        KeySchema=[
            {
                'AttributeName': 'interactive_id',
                'KeyType': 'HASH'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'interactive_id',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )

    curriculum_table = dynamodb.create_table(
        TableName='Curriculums',
        KeySchema=[
            {
                'AttributeName': 'curriculum_id',
                'KeyType': 'HASH'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'curriculum_id',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )

def delete_table():
    dynamodb = boto3.resource('dynamodb',  aws_access_key_id="anything", aws_secret_access_key="anything", 
                            region_name='us-east-1', endpoint_url="http://localhost:8000")
    dynamodb.Table('Quizzes').delete()
    dynamodb.Table('Units').delete()
    dynamodb.Table('Lessons').delete()
    dynamodb.Table('Interactives').delete()
    dynamodb.Table('Curriculums').delete()

# delete_table()
create_table()