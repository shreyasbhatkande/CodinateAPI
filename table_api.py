import boto3
import datetime;
import json

db_resource = boto3.resource('dynamodb', aws_access_key_id="anything", aws_secret_access_key="anything", 
                          region_name='us-east-1', endpoint_url="http://localhost:8000")
db_client = boto3.client("dynamodb", aws_access_key_id="anything", aws_secret_access_key="anything", 
                          region_name='us-east-1', endpoint_url="http://localhost:8000")
table = db_resource.Table('Quizzes')

def print_table(table_name):
    response = db_client.scan(TableName=table_name)
    for item in response['Items']:
        print(item)

def create_new_quiz(input_json, output_json):
    data = json.load(input_json)
    for k, v in data["Questions"].items():
        if len(v['Choices']) == 0:
            raise Exception("No options were given for " + k) 
        if not isinstance(v['Answer'], int):
            raise Exception(str(v['Answer']) + " is not a valid index")
        if v['Answer'] >= len(v['Choices']) or v['Answer'] < 0:
            raise Exception("Answer index " + str(v['Answer']) + " out of bounds")
    quiz_name = data['name']
    quiz_id = str(datetime.datetime.utcnow().hour) + str(datetime.datetime.utcnow().minute) + str(datetime.datetime.utcnow().second) \
    + str(datetime.datetime.utcnow().day) + str(datetime.datetime.utcnow().month) + str(datetime.datetime.utcnow().year)[2:]
    for k, v in data['Questions'].items():
        question_number = int(k)
        answer = int(v['Answer'])
        choices = v['Choices']
        question = v['Question']
        table.put_item(
            Item={
                'quiz_id': quiz_id,
                'question_number': question_number,
                'question': question,
                'choices': choices,
                'answer': answer,
                'quiz_name': quiz_name
            }
        )
    out_json = {'quiz_id': quiz_id, 'quiz_name': quiz_name}
    json.dump(out_json, fp=output_json)

def get_quizzes(output_json):
    response = db_client.scan(TableName='Quizzes', Select='SPECIFIC_ATTRIBUTES', ProjectionExpression='quiz_id,quiz_name')
    out_dict = {}
    for r in response['Items']:
        out_dict[list(r['quiz_id'].values())[0]] = list(r['quiz_name'].values())[0]
    while 'LastEvaluatedKey' in response.keys():
        for r in response['Items']:
            response = db_client.scan(TableName='Quizzes', Select='SPECIFIC_ATTRIBUTES', ProjectionExpression='quiz_id,quiz_name',
                                      ExclusiveStartKey=response['LastEvaluatedKey'])
            out_dict[list(r['quiz_id'].values())[0]] = list(r['quiz_name'].values())[0]
    json.dump(out_dict, fp=output_json)
    
output_file = open('out.json', 'w')
add_quiz_input = open('add_quiz_data.json')
# create_new_quiz(add_quiz_input, output_file)
# get_quizzes(output_file)
output_file.close()
add_quiz_input.close()