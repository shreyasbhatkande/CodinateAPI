import boto3
import datetime;
import json
from boto3.dynamodb.conditions import Key


db_resource = boto3.resource('dynamodb', aws_access_key_id="anything", aws_secret_access_key="anything", 
                          region_name='us-east-1', endpoint_url="http://localhost:8000")
db_client = boto3.client("dynamodb", aws_access_key_id="anything", aws_secret_access_key="anything", 
                          region_name='us-east-1', endpoint_url="http://localhost:8000")
quiz_table = db_resource.Table('Quizzes')
interactive_table = db_resource.Table('Interactives')


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
    desc = data['description']
    quiz_id = str(datetime.datetime.utcnow().hour) + str(datetime.datetime.utcnow().minute) + str(datetime.datetime.utcnow().second) \
    + str(datetime.datetime.utcnow().day) + str(datetime.datetime.utcnow().month) + str(datetime.datetime.utcnow().year)[2:]
    for k, v in data['Questions'].items():
        question_number = int(k)
        answer = int(v['Answer'])
        choices = v['Choices']
        question = v['Question']
        quiz_table.put_item(
            Item={
                'quiz_id': quiz_id,
                'description': desc,
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
        response = db_client.scan(TableName='Quizzes', Select='SPECIFIC_ATTRIBUTES', ProjectionExpression='quiz_id,quiz_name',
                                      ExclusiveStartKey=response['LastEvaluatedKey'])
        for r in response['Items']:
            out_dict[list(r['quiz_id'].values())[0]] = list(r['quiz_name'].values())[0]
    json.dump(out_dict, fp=output_json)
    
    
def check_quiz(input_json, output_json):
    inp_dict = json.load(input_json)
    response = quiz_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='answer,question_number', 
                               KeyConditionExpression=Key('quiz_id').eq(inp_dict['quiz_id']))
    qa_pairs = {}
    for item in response['Items']:
        qa_pairs[int(item['question_number'])] = int(item['answer'])
    while 'LastEvaluatedKey' in response.keys():
        response = quiz_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='answer,question_number', 
                               KeyConditionExpression=Key('quiz_id').eq(inp_dict['quiz_id']), ExclusiveStartKey=response['LastEvaluatedKey'])
        for item in response['Items']:
            qa_pairs[int(item['question_number'])] = int(item['answer'])
    tot_answers = len(qa_pairs)
    correct_answers = 0
    wrong_answers = 0
    out_list = []
    for index, answer in enumerate(inp_dict['Answers']):
        if qa_pairs[index + 1] == answer:
            out_list.append(True)
            correct_answers += 1
        else:
            out_list.append(False)
            wrong_answers += 1
    out_dict = {'tot_answers': tot_answers, 'correct_answers': correct_answers, 'wrong_answers': wrong_answers, 'record': out_list}
    json.dump(out_dict, fp=output_json)
    
    
def add_choice(input_json, output_json):
    inp_dict = json.load(input_json)
    key_exp = Key('quiz_id').eq(inp_dict['quiz_id'])
    key_exp &= Key('question_number').eq(inp_dict['question_number'])
    curr_list = []
    response = quiz_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='choices', 
                               KeyConditionExpression=key_exp)
    if not (len(response['Items']) > 0):
        while 'LastEvaluatedKey' in response.keys():
            response = quiz_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='choices', 
                                KeyConditionExpression=key_exp, ExclusiveStartKey=response['LastEvaluatedKey'])
    for item in response['Items']:
        for choice in item['choices']:
            curr_list.append(choice)
    old_list = curr_list.copy()
    for choice in inp_dict['new_options']:
        curr_list.append(choice)
    quiz_table.update_item(Key={'quiz_id': inp_dict['quiz_id'], 'question_number': inp_dict['question_number']},
                           UpdateExpression='set choices = :qs',
                           ExpressionAttributeValues={
                               ":qs": curr_list
                           })
    out_dict = {}
    out_dict['options'] = curr_list
    out_dict['old_options'] = old_list
    out_dict['quiz_id'] = inp_dict['quiz_id']
    out_dict['question_number'] = inp_dict['question_number']
    json.dump(out_dict, output_json)
    

def remove_choice(input_json, output_json):
    inp_dict = json.load(input_json)
    key_exp = Key('quiz_id').eq(inp_dict['quiz_id'])
    key_exp &= Key('question_number').eq(inp_dict['question_number'])
    curr_list = []
    response = quiz_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='choices', 
                               KeyConditionExpression=key_exp)
    if not len(response['Items']) > 0:
        while 'LastEvaluatedKey' in response.keys():
            response = quiz_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='choices', 
                                KeyConditionExpression=key_exp, ExclusiveStartKey=response['LastEvaluatedKey'])
    for item in response['Items']:
            for choice in item['choices']:
                curr_list.append(choice)
    new_list = []
    for c, v in enumerate(curr_list):
        if c in inp_dict['remove_indexes']:
            continue
        new_list.append(v)
    quiz_table.update_item(Key={'quiz_id': inp_dict['quiz_id'], 'question_number': inp_dict['question_number']},
                           UpdateExpression='set choices = :qs',
                           ExpressionAttributeValues={
                               ":qs": new_list
                           })
    out_dict = {}
    out_dict['options'] = new_list
    out_dict['old_options'] = curr_list
    out_dict['quiz_id'] = inp_dict['quiz_id']
    out_dict['question_number'] = inp_dict['question_number']
    json.dump(out_dict, output_json)
    
    
def change_answer(input_json, output_json):
    inp_dict = json.load(input_json)
    key_exp = Key('quiz_id').eq(inp_dict['quiz_id'])
    key_exp &= Key('question_number').eq(inp_dict['question_number'])
    response = quiz_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='choices,answer', 
                               KeyConditionExpression=key_exp)
    if not len(response['Items']) > 0:
        while 'LastEvaluatedKey' in response.keys():
            response = quiz_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='choices,answer', 
                                KeyConditionExpression=key_exp, ExclusiveStartKey=response['LastEvaluatedKey'])
    old_answer = int(response['Items'][0]['answer'])
        
    if inp_dict['new_answer'] < 0 or inp_dict['new_answer'] >= len(response['Items'][0]['choices']):
        raise Exception("Answer index " + str(inp_dict['new_answer']) + " out of bounds")
    quiz_table.update_item(Key={'quiz_id': inp_dict['quiz_id'], 'question_number': inp_dict['question_number']},
                           UpdateExpression='set answer = :a',
                           ExpressionAttributeValues={
                               ":a": inp_dict['new_answer']
                           })
    out_dict = {}
    out_dict['answer'] = inp_dict['new_answer']
    out_dict['old_answer'] = old_answer
    out_dict['quiz_id'] = inp_dict['quiz_id']
    out_dict['question_number'] = inp_dict['question_number']
    json.dump(out_dict, output_json)
    
    
def change_quiz_name(input_json, output_json):
    inp_dict = json.load(input_json)
    response = quiz_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='question_number,quiz_name', 
                               KeyConditionExpression=Key('quiz_id').eq(inp_dict['quiz_id']))
    old_name = ""
    for item in response['Items']:
        old_name = item['quiz_name']
        key_exp = Key('quiz_id').eq(inp_dict['quiz_id'])
        key_exp &= Key('question_number').eq(item['question_number'])
        quiz_table.update_item(Key={'quiz_id': inp_dict['quiz_id'], 'question_number': item['question_number']},
                           UpdateExpression='set quiz_name = :n',
                           ExpressionAttributeValues={
                               ":n": inp_dict['new_name']
                           })
    while 'LastEvaluatedKey' in response.keys():
        response = quiz_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='question_number,quiz_name', 
                               KeyConditionExpression=Key('quiz_id').eq(inp_dict['quiz_id']), ExclusiveStartKey=response['LastEvaluatedKey'])
        for item in response['Items']:
            key_exp = Key('quiz_id').eq(inp_dict['quiz_id'])
            key_exp &= Key('question_number').eq(item['question_number'])
            quiz_table.update_item(Key={'quiz_id': inp_dict['quiz_id'], 'question_number': item['question_number']},
                            UpdateExpression='set quiz_name = :n',
                            ExpressionAttributeValues={
                                ":n": inp_dict['new_name']
                            })
    out_dict = {}
    out_dict['name'] = inp_dict['new_name']
    out_dict['old_name'] = old_name
    out_dict['quiz_id'] = inp_dict['quiz_id']
    json.dump(out_dict, output_json)


def change_question(input_json, output_json):
    inp_dict = json.load(input_json)
    key_exp = Key('quiz_id').eq(inp_dict['quiz_id'])
    key_exp &= Key('question_number').eq(inp_dict['question_number'])
    response = quiz_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='question', 
                               KeyConditionExpression=key_exp)
    if not len(response['Items']) > 0:
        while 'LastEvaluatedKey' in response.keys():
            response = quiz_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='question', 
                                KeyConditionExpression=key_exp, ExclusiveStartKey=response['LastEvaluatedKey'])
    old_question = response['Items'][0]['question']
    quiz_table.update_item(Key={'quiz_id': inp_dict['quiz_id'], 'question_number': inp_dict['question_number']},
                           UpdateExpression='set question = :q',
                           ExpressionAttributeValues={
                               ":q": inp_dict['new_question']
                           })
    out_dict = {}
    out_dict['question'] = inp_dict['new_question']
    out_dict['old_question'] = old_question
    out_dict['quiz_id'] = inp_dict['quiz_id']
    out_dict['question_number'] = inp_dict['question_number']
    json.dump(out_dict, output_json)
    
    
def change_quiz_desc(input_json, output_json):
    inp_dict = json.load(input_json)
    response = quiz_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='question_number,description', 
                               KeyConditionExpression=Key('quiz_id').eq(inp_dict['quiz_id']))
    old_desc = ""
    for item in response['Items']:
        old_desc = item['description']
        key_exp = Key('quiz_id').eq(inp_dict['quiz_id'])
        key_exp &= Key('question_number').eq(item['question_number'])
        quiz_table.update_item(Key={'quiz_id': inp_dict['quiz_id'], 'question_number': item['question_number']},
                           UpdateExpression='set description = :d',
                           ExpressionAttributeValues={
                               ":d": inp_dict['new_desc']
                           })
    while 'LastEvaluatedKey' in response.keys():
        response = quiz_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='question_number,description', 
                               KeyConditionExpression=Key('quiz_id').eq(inp_dict['quiz_id']), ExclusiveStartKey=response['LastEvaluatedKey'])
        for item in response['Items']:
            key_exp = Key('quiz_id').eq(inp_dict['quiz_id'])
            key_exp &= Key('question_number').eq(item['question_number'])
            quiz_table.update_item(Key={'quiz_id': inp_dict['quiz_id'], 'question_number': item['question_number']},
                            UpdateExpression='set description = :d',
                            ExpressionAttributeValues={
                                ":d": inp_dict['new_desc']
                            })
    out_dict = {}
    out_dict['description'] = inp_dict['new_desc']
    out_dict['old_description'] = old_desc
    out_dict['quiz_id'] = inp_dict['quiz_id']
    json.dump(out_dict, output_json)
    
    
def get_quiz(input_json, output_json):
    inp_dict = json.load(input_json)
    out_dict = {}
    response = quiz_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='description,question_number,question,choices,quiz_name', 
                               KeyConditionExpression=Key('quiz_id').eq(inp_dict['quiz_id']))
    for item in response['Items']:
        out_dict[int(item['question_number'])] = {'description':item['description'], 'question':item['question'], 'choices':item['choices'],
                                            'quiz_name':item['quiz_name']}
    while 'LastEvaluatedKey' in response.keys():
        response = quiz_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='description,question_number,question,choices,quiz_name', 
                               KeyConditionExpression=Key('quiz_id').eq(inp_dict['quiz_id']), ExclusiveStartKey=response['LastEvaluatedKey'])
        for item in response['Items']:
            out_dict[int(item['question_number'])] = {'description':item['description'], 'question':item['question'], 'choices':item['choices'],
                                                'quiz_name':item['quiz_name']}
    json.dump(out_dict, output_json)
        
        
def get_question(input_json, output_json):
    inp_dict = json.load(input_json)
    key_exp = Key('quiz_id').eq(inp_dict['quiz_id'])
    key_exp &= Key('question_number').eq(inp_dict['question_number'])
    response = quiz_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='description,question_number,question,choices,quiz_name,answer', 
                               KeyConditionExpression=key_exp)
    if not len(response['Items']) > 0:
        while 'LastEvaluatedKey' in response.keys():
            response = quiz_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='description,question,choices,quiz_name,answer', 
                               KeyConditionExpression=key_exp, ExclusiveStartKey=response['LastEvaluatedKey'])
    out_dict = response['Items'][0].copy()
    out_dict['quiz_id'] = inp_dict['quiz_id']
    out_dict['question_number'] = int(inp_dict['question_number'])
    out_dict['answer'] = int(response['Items'][0]['answer'])
    json.dump(out_dict, output_json)
    
    
def create_interactive(input_json, associated_data, output_json):
    inp_dict = json.load(input_json)
    interactive_id = str(datetime.datetime.utcnow().hour) + str(datetime.datetime.utcnow().minute) + str(datetime.datetime.utcnow().second) \
    + str(datetime.datetime.utcnow().day) + str(datetime.datetime.utcnow().month) + str(datetime.datetime.utcnow().year)[2:]
    interactive_table.put_item(
        Item={
            "interactive_id": interactive_id,
            'interactive_url': inp_dict['url'],
            'interactive_name': inp_dict['name'],
            'description': inp_dict['description'],
            'associated_data': json.load(associated_data)
        }
    )
    out_dict = {'interactive_id': interactive_id, 'name': inp_dict['name']}
    json.dump(out_dict, output_json)
    
    
def get_all_interactives(output_json):
    response = db_client.scan(TableName='Interactives', Select='SPECIFIC_ATTRIBUTES', ProjectionExpression='interactive_id,interactive_name')
    out_dict = {}
    for item in response['Items']:
        out_dict[item['interactive_id']['S']] = item['interactive_name']['S']
    while 'LastEvaluatedKey' in response.keys():
        response = db_client.scan(TableName='Interactives', Select='SPECIFIC_ATTRIBUTES', ProjectionExpression='interactive_id,interactive_name',
                                  ExclusiveStartKey=response['LastEvaluatedKey'])
        for item in response['Items']:
            out_dict[item['interactive_id']['S']] = item['interactive_name']['S']
    json.dump(out_dict, fp=output_json)
        

def change_url(input_json, output_json):
    inp_dict = json.load(input_json)
    key_exp = Key('interactive_id').eq(inp_dict['interactive_id'])
    response = interactive_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='interactive_url', 
                               KeyConditionExpression=key_exp)
    if not len(response['Items']) > 0:
        while 'LastEvaluatedKey' in response.keys():
            response = interactive_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='interactive_url', 
                                KeyConditionExpression=key_exp, ExclusiveStartKey=response['LastEvaluatedKey'])
    old_url = response['Items'][0]['interactive_url']
    interactive_table.update_item(Key={'interactive_id': inp_dict['interactive_id']},
                           UpdateExpression='set interactive_url = :u',
                           ExpressionAttributeValues={
                               ":u": inp_dict['new_url']
                           })
    out_dict = {}
    out_dict['url'] = inp_dict['new_url']
    out_dict['old_url'] = old_url
    out_dict['interactive_id'] = inp_dict['interactive_id']
    json.dump(out_dict, output_json)
    

def change_interactive_name(input_json, output_json):
    inp_dict = json.load(input_json)    
    key_exp = Key('interactive_id').eq(inp_dict['interactive_id'])
    response = interactive_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='interactive_name', 
                               KeyConditionExpression=key_exp)
    if not len(response['Items']) > 0:
        while 'LastEvaluatedKey' in response.keys():
            response = interactive_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='interactive_name', 
                                KeyConditionExpression=key_exp, ExclusiveStartKey=response['LastEvaluatedKey'])
    old_name = response['Items'][0]['interactive_name']
    interactive_table.update_item(Key={'interactive_id': inp_dict['interactive_id']},
                           UpdateExpression='set interactive_name = :u',
                           ExpressionAttributeValues={
                               ":u": inp_dict['new_name']
                           })
    out_dict = {}
    out_dict['name'] = inp_dict['new_name']
    out_dict['old_name'] = old_name
    out_dict['interactive_id'] = inp_dict['interactive_id']
    json.dump(out_dict, output_json)
    
    
def change_interactive_description(input_json, output_json):
    inp_dict = json.load(input_json)    
    key_exp = Key('interactive_id').eq(inp_dict['interactive_id'])
    response = interactive_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='description', 
                               KeyConditionExpression=key_exp)
    if not len(response['Items']) > 0:
        while 'LastEvaluatedKey' in response.keys():
            response = interactive_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='description', 
                                KeyConditionExpression=key_exp, ExclusiveStartKey=response['LastEvaluatedKey'])
    old_description = response['Items'][0]['description']
    interactive_table.update_item(Key={'interactive_id': inp_dict['interactive_id']},
                           UpdateExpression='set description = :u',
                           ExpressionAttributeValues={
                               ":u": inp_dict['new_description']
                           })
    out_dict = {}
    out_dict['description'] = inp_dict['new_description']
    out_dict['old_description'] = old_description
    out_dict['interactive_id'] = inp_dict['interactive_id']
    json.dump(out_dict, output_json)
    
    
def change_interactive_json(input_file, new_json, output_json):
    inp_dict = json.load(input_file)
    key_exp = Key('interactive_id').eq(inp_dict['interactive_id'])
    new_json = json.load(new_json)
    response = interactive_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='associated_data', 
                               KeyConditionExpression=key_exp)
    if not len(response['Items']) > 0:
        while 'LastEvaluatedKey' in response.keys():
            response = interactive_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='associated_data', 
                                KeyConditionExpression=key_exp, ExclusiveStartKey=response['LastEvaluatedKey'])
    old_data = response['Items'][0]['associated_data']
    interactive_table.update_item(Key={'interactive_id': inp_dict['interactive_id']},
                           UpdateExpression='set associated_data = :u',
                           ExpressionAttributeValues={
                               ":u": new_json
                           })
    out_dict = {}
    out_dict['interactive_id'] = inp_dict['interactive_id']
    out_dict['old_data'] = old_data
    out_dict['new_data'] = new_json
    json.dump(out_dict, output_json)
    
    
def get_interactive(input_json, output_json):
    inp_dict = json.load(input_json)
    key_exp = Key('interactive_id').eq(inp_dict['interactive_id'])
    response = interactive_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='interactive_url,interactive_name,description,associated_data', 
                               KeyConditionExpression=key_exp)
    if not len(response['Items']) > 0:
        while 'LastEvaluatedKey' in response.keys():
            response = interactive_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='interactive_url,interactive_name,description,associated_data', 
                               KeyConditionExpression=key_exp, ExclusiveStartKey=response['LastEvaluatedKey'])
    json.dump(response['Items'][0], output_json)
    
    
def add_question(input_json, output_json):
    inp_dict = json.load(input_json) 
    start_index = inp_dict['question_number']
    if start_index != 0:
        start_index -= 1
    key_exp = Key('quiz_id').eq(inp_dict['quiz_id'])
    key_exp &= Key('question_number').gte(start_index)
    response = quiz_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='question_number,question,answer,choices,description,quiz_name', 
                               KeyConditionExpression=key_exp)
    saved_attr = {}
    out_dict = {'changed_questions': []}
    if len(response['Items']) > 0:
        for item in reversed(response['Items']):
            if int(item['question_number']) < inp_dict['question_number']:
                continue
            quiz_table.put_item(
            Item={
                'quiz_id': inp_dict['quiz_id'],
                'description': item['description'],
                'question_number': int(item['question_number']) + 1,
                'question': item['question'],
                'choices': item['choices'],
                'answer': item['answer'],
                'quiz_name': item['quiz_name']
            }
            )
            out_dict['changed_questions'].append(int(item['question_number']))
            saved_attr['description'] = item['description']
            saved_attr['quiz_name'] = item['quiz_name']
            quiz_table.delete_item(Key={'quiz_id': inp_dict['quiz_id'], 'question_number': item['question_number']})
    while 'LastEvaluatedKey' in response.keys():
        response = quiz_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='question_number,question,answer,choices,description,quiz_name', 
                               KeyConditionExpression=key_exp, ExclusiveStartKey=response['LastEvaluatedKey'])
        if len(response['Items']) > 0:
            for item in reversed(response['Items']):
                if int(item['question_number']) < inp_dict['question_number']:
                    continue
                quiz_table.put_item(
                Item={
                    'quiz_id': inp_dict['quiz_id'],
                    'description': item['description'],
                    'question_number': int(item['question_number']) + 1,
                    'question': item['question'],
                    'choices': item['choices'],
                    'answer': item['answer'],
                    'quiz_name': item['quiz_name']
                }
                )
                out_dict['changed_questions'].append(int(item['question_number']))
                quiz_table.delete_item(Key={'quiz_id': inp_dict['quiz_id'], 'question_number': item['question_number']})
    quiz_table.put_item(
        Item={
            'quiz_id': inp_dict['quiz_id'],
            'description': saved_attr['description'],
            'question_number': inp_dict['question_number'],
            'question': inp_dict['question'],
            'choices': inp_dict['choices'],
            'answer': inp_dict['answer'],
            'quiz_name': saved_attr['quiz_name']
        }   
    )
    out_dict['quiz_id'] = inp_dict['quiz_id']
    json.dump(out_dict, output_json)
    
    
output_file = open('out.json', 'w')
add_quiz_input = open('add_quiz_data.json')
check_quiz_input = open('check_quiz_data.json')
add_choice_input = open('add_choice_data.json')
remove_choice_input = open('remove_choice_data.json')
change_answer_input = open('change_answer_data.json')
change_quiz_name_input = open('change_quiz_name_data.json')
change_question_input = open('change_question_data.json')
change_quiz_desc_input = open('change_quiz_desc_data.json')
get_quiz_input = open('get_quiz_data.json')
get_question_input = open("get_question_data.json")
create_interactive_input = open('create_interactive_data.json')
associated_interactive_data = open('associated_interactive_data.json')
change_url_input = open('change_url_data.json')
change_interactive_name_input = open('change_interactive_name_data.json')
change_interactive_description_input = open('change_interactive_description_data.json')
change_interactive_json_input = open('change_interactive_json_data.json')
new_interactive_json = open('new_interactive_json_data.json')
get_interactive_input = open('get_interactive_data.json')
add_question_input = open('add_question_data.json')
print_table('Quizzes')
# create_new_quiz(add_quiz_input, output_file)
# get_quizzes(output_file)
# check_quiz(check_quiz_input, output_file)
# add_choice(add_choice_input, output_file)
# remove_choice(remove_choice_input, output_file)
# change_answer(change_answer_input, output_file)
# change_quiz_name(change_quiz_name_input, output_file) 
# change_question(change_question_input, output_file)
# change_quiz_desc(change_quiz_desc_input, output_file) 
# get_quiz(get_quiz_input, output_file)
# get_question(get_question_input, output_file)
# create_interactive(create_interactive_input, associated_interactive_data, output_file)
# get_all_interactives(output_file)
# change_url(change_url_input, output_file)
# change_interactive_name(change_interactive_name_input, output_file)
# change_interactive_description(change_interactive_description_input, output_file)
# change_interactive_json(change_interactive_json_input, new_interactive_json, output_file)
# get_interactive(get_interactive_input, output_file)
# add_question(add_question_input, output_file)
output_file.close()
add_quiz_input.close()
check_quiz_input.close()
add_choice_input.close()
remove_choice_input.close()
change_answer_input.close()
change_quiz_name_input.close()
change_question_input.close()
change_quiz_desc_input.close()
get_quiz_input.close()
get_question_input.close()
create_interactive_input.close()
associated_interactive_data.close()
change_url_input.close()
change_interactive_name_input.close()
change_interactive_description_input.close()
change_interactive_json_input.close()
new_interactive_json.close()
get_interactive_input.close()
add_question_input.close()