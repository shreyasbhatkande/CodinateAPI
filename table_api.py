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
curriculum_table = db_resource.Table("Curriculums")
lesson_table = db_resource.Table('Lessons')
unit_table = db_resource.Table('Units')

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
    while not len(response['Items']) > 0 and 'LastEvaluatedKey' in response.keys():
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
    while not len(response['Items']) > 0 and 'LastEvaluatedKey' in response.keys():
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
    while not len(response['Items']) > 0 and 'LastEvaluatedKey' in response.keys():
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
    while not len(response['Items']) > 0 and 'LastEvaluatedKey' in response.keys():
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
    while not len(response['Items']) > 0 and 'LastEvaluatedKey' in response.keys():
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
    while not len(response['Items']) > 0 and 'LastEvaluatedKey' in response.keys():
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
    while not len(response['Items']) > 0 and 'LastEvaluatedKey' in response.keys():
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
    while not len(response['Items']) > 0 and 'LastEvaluatedKey' in response.keys():
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
    while not len(response['Items']) > 0 and 'LastEvaluatedKey' in response.keys():
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
    while not len(response['Items']) > 0 and 'LastEvaluatedKey' in response.keys():
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
    while not len(response['Items']) > 0 and 'LastEvaluatedKey' in response.keys():
        response = quiz_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='question_number,question,answer,choices,description,quiz_name', 
                               KeyConditionExpression=key_exp, ExclusiveStartKey=response['LastEvaluatedKey'])
    saved_attr = {}
    out_dict = {'changed_questions': []}
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
    
    
def remove_question(input_json, output_json):
    inp_dict = json.load(input_json)
    key_exp = Key('quiz_id').eq(inp_dict['quiz_id'])
    key_exp &= Key('question_number').gt(inp_dict['remove_num'])
    k2 = Key('quiz_id').eq(inp_dict['quiz_id'])
    k2 &= Key('question_number').eq(inp_dict['remove_num'])
    response = quiz_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='question,answer,choices', 
                               KeyConditionExpression=k2)
    while not len(response['Items']) > 0 and 'LastEvaluatedKey' in response.keys():
        response = quiz_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='question,answer,choices', 
                               KeyConditionExpression=k2, ExclusiveStartKey=response['LastEvaluatedKey'])
    removed_question = response['Items'][0]
    removed_question['answer'] = int(removed_question['answer'])
    quiz_table.delete_item(Key={'quiz_id': inp_dict['quiz_id'], 'question_number': inp_dict['remove_num']})
    response = quiz_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='question_number,question,answer,choices,description,quiz_name', 
                               KeyConditionExpression=key_exp)
    out_dict = {'changed_questions':[]}
    for item in response['Items']:
        quiz_table.put_item(
        Item={
            'quiz_id': inp_dict['quiz_id'],
            'description': item['description'],
            'question_number': int(item['question_number']) - 1,
            'question': item['question'],
            'choices': item['choices'],
            'answer': item['answer'],
            'quiz_name': item['quiz_name']
            }
        )
        out_dict['changed_questions'].append(int(item['question_number']))
        quiz_table.delete_item(Key={'quiz_id': inp_dict['quiz_id'], 'question_number': item['question_number']})
    while 'LastEvaluatedKey' in response.keys():
        response = quiz_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='question_number,question,answer,choices,description,quiz_name', 
                               KeyConditionExpression=key_exp, ExclusiveStartKey=response['LastEvaluatedKey'])
        for item in response['Items']:
            quiz_table.put_item(
                Item={
                    'quiz_id': inp_dict['quiz_id'],
                    'description': item['description'],
                    'question_number': int(item['question_number']) - 1,
                    'question': item['question'],
                    'choices': item['choices'],
                    'answer': item['answer'],
                    'quiz_name': item['quiz_name']
                }
            )
            out_dict['changed_questions'].append(int(item['question_number']))
            quiz_table.delete_item(Key={'quiz_id': inp_dict['quiz_id'], 'question_number': item['question_number']})
    out_dict['quiz_id'] = inp_dict['quiz_id']
    out_dict['removed_question'] = removed_question
    json.dump(out_dict, output_json)
    
    
def change_question_order(input_json, output_json):
    inp_dict = json.load(input_json)
    original_pos = inp_dict['question_number']
    new_pos = inp_dict['new_pos']
    if original_pos == new_pos:
        raise Exception('Both positions given are the same')
    k2 = Key('quiz_id').eq(inp_dict['quiz_id'])
    k2 &= Key('question_number').eq(original_pos)
    response = quiz_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='question,answer,choices,description,quiz_name', 
                               KeyConditionExpression=k2)
    while not len(response['Items']) > 0 and 'LastEvaluatedKey' in response.keys():
        response = quiz_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='question,answer,choices,description,quiz_name', 
                               KeyConditionExpression=k2, ExclusiveStartKey=response['LastEvaluatedKey'])
    removed_question = response['Items'][0]
    removed_question['quiz_id'] = inp_dict['quiz_id']
    removed_question['question_number'] = new_pos
    out_dict = {'changed_indexes': []}
    quiz_table.delete_item(Key={'quiz_id': inp_dict['quiz_id'], 'question_number': original_pos})
    if original_pos > new_pos:
        key_exp = Key('quiz_id').eq(inp_dict['quiz_id'])
        key_exp &= Key('question_number').between(new_pos, original_pos - 1)    
        response = quiz_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='question_number,question,answer,choices,description,quiz_name', 
                               KeyConditionExpression=key_exp)
        for item in reversed(response['Items']):
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
            out_dict['changed_indexes'].append(int(item['question_number']))
        while 'LastEvaluatedKey' in response.keys():
            response = quiz_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='question_number,question,answer,choices,description,quiz_name', 
                               KeyConditionExpression=key_exp, ExclusiveStartKey=response['LastEvaluatedKey'])
            for item in reversed(response['Items']):
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
                out_dict['changed_indexes'].append(int(item['question_number']))
    else:
        key_exp = Key('quiz_id').eq(inp_dict['quiz_id'])
        key_exp &= Key('question_number').between(original_pos + 1, new_pos)    
        response = quiz_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='question_number,question,answer,choices,description,quiz_name', 
                               KeyConditionExpression=key_exp)
        for item in response['Items']:
            quiz_table.put_item(
                Item={
                    'quiz_id': inp_dict['quiz_id'],
                    'description': item['description'],
                    'question_number': int(item['question_number']) - 1,
                    'question': item['question'],
                    'choices': item['choices'],
                    'answer': item['answer'],
                    'quiz_name': item['quiz_name']
                }
            )
            out_dict['changed_indexes'].append(int(item['question_number']))
        while 'LastEvaluatedKey' in response.keys():
            response = quiz_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='question_number,question,answer,choices,description,quiz_name', 
                               KeyConditionExpression=key_exp, ExclusiveStartKey=response['LastEvaluatedKey'])
            for item in response['Items']:
                quiz_table.put_item(
                    Item={
                        'quiz_id': inp_dict['quiz_id'],
                        'description': item['description'],
                        'question_number': int(item['question_number']) - 1,
                        'question': item['question'],
                        'choices': item['choices'],
                        'answer': item['answer'],
                        'quiz_name': item['quiz_name']
                    }
                )
                out_dict['changed_indexes'].append(int(item['question_number']))
    quiz_table.put_item(
        Item=removed_question
    )    
    out_dict['quiz_id'] = inp_dict['quiz_id']
    json.dump(out_dict, output_json)
        
        
def create_curriculum(input_json, output_json):
    inp_dict = json.load(input_json)
    out_dict = {}
    out_dict['unit_pairings'] = {}
    out_dict['lesson_pairings'] = {}
    unit_name_id = {}
    curriculum_id =  str(datetime.datetime.utcnow().hour) + str(datetime.datetime.utcnow().minute) + str(datetime.datetime.utcnow().second) \
    + str(datetime.datetime.utcnow().day) + str(datetime.datetime.utcnow().month) + str(datetime.datetime.utcnow().year)[2:]
    out_dict['curriculum_id'] = curriculum_id
    curriculum_table.put_item(
        Item={
            'curriculum_id': curriculum_id,
            'description': inp_dict['curriculum_info']['description'],
            'curriculum_name': inp_dict['curriculum_info']['name'],
            'image': inp_dict['curriculum_info']['image'],
        }
    )
    for lesson in inp_dict['curriculum_elements']:
        if lesson['unit'] not in unit_name_id:
            unit_name_id[lesson["unit"]] = {}
            unit_name_id[lesson["unit"]]['id'] = curriculum_id
        unit_name_id[lesson['unit']]['description'] = lesson['unit_description']
        unit_name_id[lesson['unit']]['unit_num'] = lesson['unitOrderNum']
        out_dict['lesson_pairings'][lesson['name']] = unit_name_id[lesson['unit']]['id'] + "-" + str(lesson['unitOrderNum'])
        lesson_table.put_item(
            Item={
                'lesson_id': curriculum_id + "-" + str(lesson['unitOrderNum']),
                'lesson_number': lesson['lessonOrderNum'],
                'video': lesson['videoLink'],
                'lesson_name': lesson['name'],
                'lesson_desc': lesson['lessonDescription'],
                'quizzes': [],
                'interactives': [],
            }
        )
    for k, v in unit_name_id.items():
        out_dict['unit_pairings'][k] = v['id']
        unit_table.put_item(
            Item={
                'unit_id': v['id'],
                'unit_number': v['unit_num'],
                'description': v['description'],
                'unit_name': k,
                'quizzes': [],
            }
        )
    json.dump(out_dict, output_json)
    
    
def get_all_curriculums(output_json):
    response = db_client.scan(TableName='Curriculums', Select='SPECIFIC_ATTRIBUTES', ProjectionExpression='curriculum_name,curriculum_id')
    out_dict = {}
    for r in response['Items']:
        out_dict[r['curriculum_id']['S']] = r['curriculum_name']['S']
    while 'LastEvaluatedKey' in response.keys():
        response = db_client.scan(TableName='Curriculums', Select='SPECIFIC_ATTRIBUTES', ProjectionExpression='curriculum_name,curriculum_id', 
                                  ExclusiveStartKey=response['LastEvaluatedKey'])
        for r in response['Items']:
            out_dict[r['curriculum_id']['S']] = r['curriculum_name']['S']
    json.dump(out_dict, fp=output_json)
    
    
def change_curriculum_name(input_json, output_json):
    inp_dict = json.load(input_json)
    key_exp = Key('curriculum_id').eq(inp_dict['curriculum_id'])
    response = curriculum_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='curriculum_name', 
                               KeyConditionExpression=key_exp)
    while not len(response['Items']) > 0 and 'LastEvaluatedKey' in response.keys():
        response = curriculum_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='curriculum_name', 
                            KeyConditionExpression=key_exp, ExclusiveStartKey=response['LastEvaluatedKey'])
    old_name = response['Items'][0]['curriculum_name']
    curriculum_table.update_item(Key={'curriculum_id': inp_dict['curriculum_id']},
                           UpdateExpression='set curriculum_name = :c',
                           ExpressionAttributeValues={
                               ":c": inp_dict['new_name']
                           })
    out_dict = {}
    out_dict['name'] = inp_dict['new_name']
    out_dict['old_name'] = old_name
    out_dict['curriculum_id'] = inp_dict['curriculum_id']
    json.dump(out_dict, output_json)
    
    
def change_curriculum_description(input_json, output_json):
    inp_dict = json.load(input_json)
    key_exp = Key('curriculum_id').eq(inp_dict['curriculum_id'])
    response = curriculum_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='description', 
                               KeyConditionExpression=key_exp)
    while not len(response['Items']) > 0 and 'LastEvaluatedKey' in response.keys():
        response = curriculum_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='description', 
                                KeyConditionExpression=key_exp, ExclusiveStartKey=response['LastEvaluatedKey'])
    old_desc = response['Items'][0]['description']
    curriculum_table.update_item(Key={'curriculum_id': inp_dict['curriculum_id']},
                           UpdateExpression='set description = :c',
                           ExpressionAttributeValues={
                               ":c": inp_dict['new_description']
                           })
    out_dict = {}
    out_dict['description'] = inp_dict['new_description']
    out_dict['old_desc'] = old_desc
    out_dict['curriculum_id'] = inp_dict['curriculum_id']
    json.dump(out_dict, output_json)
       
       
def change_image(input_json, output_json):
    inp_dict = json.load(input_json)
    key_exp = Key('curriculum_id').eq(inp_dict['curriculum_id'])
    response = curriculum_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='image', 
                               KeyConditionExpression=key_exp)
    while not len(response['Items']) > 0 and 'LastEvaluatedKey' in response.keys():
        response = curriculum_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='image', 
                            KeyConditionExpression=key_exp, ExclusiveStartKey=response['LastEvaluatedKey'])
    old_img = response['Items'][0]['image']
    curriculum_table.update_item(Key={'curriculum_id': inp_dict['curriculum_id']},
                           UpdateExpression='set image = :c',
                           ExpressionAttributeValues={
                               ":c": inp_dict['new_image']
                           })
    out_dict = {}
    out_dict['image'] = inp_dict['new_image']
    out_dict['old_image'] = old_img
    out_dict['curriculum_id'] = inp_dict['curriculum_id']
    json.dump(out_dict, output_json)
    
    
def get_curriculum(input_json, output_json):
    inp_dict = json.load(input_json)
    out_dict = {}
    key_exp = Key('curriculum_id').eq(inp_dict['curriculum_id'])
    unit_exp = Key('unit_id').eq(inp_dict['curriculum_id'])
    response = curriculum_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='image,description,curriculum_name', 
                               KeyConditionExpression=key_exp)
    while not len(response['Items']) > 0 and 'LastEvaluatedKey' in response.keys():
        response = curriculum_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='image,description,curriculum_name', 
                            KeyConditionExpression=key_exp, ExclusiveStartKey=response['LastEvaluatedKey'])
    response = response['Items'][0]
    out_dict['image'] = response['image']
    out_dict['description'] = response['description']
    out_dict['curriculum_name'] = response['curriculum_name']
    response = unit_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='unit_number', 
                               KeyConditionExpression=unit_exp)
    units = []
    for item in response['Items']:
        units.append(int(item['unit_number']))
    while 'LastEvaluatedKey' in response.keys():
        response = unit_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='unit_number', 
                               KeyConditionExpression=unit_exp, ExclusiveStartKey=response['LastEvaluatedKey'])
        for item in response['Items']:
            units.append(int(item['unit_number']))
    out_dict['units'] = units
    json.dump(out_dict, output_json)
    
    
def get_unit(input_json, output_json):
    inp_dict = json.load(input_json)
    out_dict = {}
    key_exp = Key('unit_id').eq(inp_dict['unit_id'])
    key_exp &= Key('unit_number').eq(inp_dict['unit_number'])
    l_exp = Key('lesson_id').eq(inp_dict['unit_id'] + '-' + str(inp_dict['unit_number']))
    out_dict['lesson_id'] = inp_dict['unit_id'] + '-' + str(inp_dict['unit_number'])
    response = unit_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='description,unit_name,quizzes', 
                               KeyConditionExpression=key_exp)
    while not len(response['Items']) > 0 and 'LastEvaluatedKey' in response.keys():
        response = unit_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='description,unit_name,quizzes', 
                               KeyConditionExpression=key_exp, ExclusiveStartKey=response['LastEvaluatedKey'])
    response = response['Items'][0]
    out_dict['description'] = response['description']
    out_dict['unit_name'] = response['unit_name']
    out_dict['quizzes'] = response['quizzes']
    response = lesson_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='lesson_number', 
                               KeyConditionExpression=l_exp)
    lessons = []
    for item in response['Items']:
        lessons.append(int(item['lesson_number']))
    while 'LastEvaluatedKey' in response.keys():
        response = lesson_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='lesson_number', 
                               KeyConditionExpression=l_exp, ExclusiveStartKey=response['LastEvaluatedKey'])
        for item in response['Items']:
            lessons.append(int(item['unit_number']))
    out_dict['lessons'] = lessons
    json.dump(out_dict, output_json)
    
    
def change_unit_description(input_json, output_json):
    inp_dict = json.load(input_json)
    key_exp = Key('unit_id').eq(inp_dict['unit_id'])
    key_exp &= Key('unit_number').eq(inp_dict['unit_number'])
    response = unit_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='description', 
                               KeyConditionExpression=key_exp)
    while not len(response['Items']) > 0 and 'LastEvaluatedKey' in response.keys():
        response = unit_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='description', 
                            KeyConditionExpression=key_exp, ExclusiveStartKey=response['LastEvaluatedKey'])
    old_description = response['Items'][0]['description']
    unit_table.update_item(Key={'unit_id': inp_dict['unit_id'], 'unit_number': inp_dict['unit_number']},
                           UpdateExpression='set description = :d',
                           ExpressionAttributeValues={
                               ":d": inp_dict['new_description']
                           })
    out_dict = {}
    out_dict['description'] = inp_dict['new_description']
    out_dict['old_description'] = old_description
    out_dict['unit_id'] = inp_dict['unit_id']
    out_dict['unit_number'] = inp_dict['unit_number']
    json.dump(out_dict, output_json)
    
    
def add_unit_quiz(input_json, output_json):
    inp_dict = json.load(input_json)
    key_exp = Key('unit_id').eq(inp_dict['unit_id'])
    key_exp &= Key('unit_number').eq(inp_dict['unit_number'])
    curr_list = []
    response = unit_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='quizzes', 
                               KeyConditionExpression=key_exp)
    while not len(response['Items']) > 0 and 'LastEvaluatedKey' in response.keys():
        response = unit_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='quizzes', 
                            KeyConditionExpression=key_exp, ExclusiveStartKey=response['LastEvaluatedKey'])
    for item in response['Items']:
        for quiz in item['quizzes']:
            curr_list.append(quiz)
    old_list = curr_list.copy()
    for choice in inp_dict['new_quizzes']:
        curr_list.append(choice)
    unit_table.update_item(Key={'unit_id': inp_dict['unit_id'], 'unit_number': inp_dict['unit_number']},
                           UpdateExpression='set quizzes = :qs',
                           ExpressionAttributeValues={
                               ":qs": curr_list
                           })
    out_dict = {}
    out_dict['quizzes'] = curr_list
    out_dict['old_quizzes'] = old_list
    out_dict['unit_id'] = inp_dict['unit_id']
    out_dict['unit_number'] = inp_dict['unit_number']
    json.dump(out_dict, output_json)
    
    
def remove_unit_quiz(input_json, output_json):
    inp_dict = json.load(input_json)
    key_exp = Key('unit_id').eq(inp_dict['unit_id'])
    key_exp &= Key('unit_number').eq(inp_dict['unit_number'])
    curr_list = []
    response = unit_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='quizzes', 
                               KeyConditionExpression=key_exp)
    while not len(response['Items']) > 0 and 'LastEvaluatedKey' in response.keys():
        response = unit_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='quizzes', 
                            KeyConditionExpression=key_exp, ExclusiveStartKey=response['LastEvaluatedKey'])
    for item in response['Items']:
        for quiz in item['quizzes']:
            curr_list.append(quiz)
    old_list = curr_list.copy()
    for choice in inp_dict['remove']:
        curr_list.remove(choice)
    unit_table.update_item(Key={'unit_id': inp_dict['unit_id'], 'unit_number': inp_dict['unit_number']},
                           UpdateExpression='set quizzes = :qs',
                           ExpressionAttributeValues={
                               ":qs": curr_list
                           })
    out_dict = {}
    out_dict['quizzes'] = curr_list
    out_dict['old_quizzes'] = old_list
    out_dict['unit_id'] = inp_dict['unit_id']
    out_dict['unit_number'] = inp_dict['unit_number']
    json.dump(out_dict, output_json)
    
        
def change_unit_name(input_json, output_json):
    inp_dict = json.load(input_json)
    key_exp = Key('unit_id').eq(inp_dict['unit_id'])
    key_exp &= Key('unit_number').eq(inp_dict['unit_number'])
    response = unit_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='unit_name', 
                               KeyConditionExpression=key_exp)
    while not len(response['Items']) > 0 and 'LastEvaluatedKey' in response.keys():
        response = unit_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='unit_name', 
                            KeyConditionExpression=key_exp, ExclusiveStartKey=response['LastEvaluatedKey'])
    old_name = response['Items'][0]['unit_name']
    unit_table.update_item(Key={'unit_id': inp_dict['unit_id'], 'unit_number': inp_dict['unit_number']},
                           UpdateExpression='set unit_name = :d',
                           ExpressionAttributeValues={
                               ":d": inp_dict['new_name']
                           })
    out_dict = {}
    out_dict['name'] = inp_dict['new_name']
    out_dict['old_name'] = old_name
    out_dict['unit_id'] = inp_dict['unit_id']
    out_dict['unit_number'] = inp_dict['unit_number']
    json.dump(out_dict, output_json)
        
        
def remove_unit(input_json, output_json):
    inp_dict = json.load(input_json)
    key_exp = Key('unit_id').eq(inp_dict['unit_id'])
    key_exp &= Key('unit_number').gt(inp_dict['unit_number'])
    k2 = Key('unit_id').eq(inp_dict['unit_id'])
    k2 &= Key('unit_number').eq(inp_dict['unit_number'])
    response = unit_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='description,unit_name,quizzes', 
                               KeyConditionExpression=k2)
    while not len(response['Items']) > 0 and 'LastEvaluatedKey' in response.keys():
        response = unit_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='description,unit_name,quizzes', 
                               KeyConditionExpression=k2,  ExclusiveStartKey=response['LastEvaluatedKey'])
    removed_unit = response['Items'][0]
    unit_table.delete_item(Key={'unit_id': inp_dict['unit_id'], 'unit_number': inp_dict['unit_number']})
    response = unit_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='description,unit_name,quizzes,unit_number', 
                               KeyConditionExpression=key_exp)
    out_dict = {'changed_units':[]}
    for item in response['Items']:
        unit_table.put_item(
        Item={
            'unit_id': inp_dict['unit_id'],
            'description': item['description'],
            'unit_number': int(item['unit_number']) - 1,
            'unit_name': item['unit_name'],
            'quizzes': item['quizzes'],
            }
        )
        out_dict['changed_units'].append(int(item['unit_number']))
        unit_table.delete_item(Key={'unit_id': inp_dict['unit_id'], 'unit_number': item['unit_number']})
    while 'LastEvaluatedKey' in response.keys():
        response = unit_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='description,unit_name,quizzes,unit_number', 
                               KeyConditionExpression=key_exp, ExclusiveStartKey=response['LastEvaluatedKey'])
        for item in response['Items']:
            unit_table.put_item(
            Item={
                'unit_id': inp_dict['unit_id'],
                'description': item['description'],
                'unit_number': int(item['unit_number']) - 1,
                'unit_name': item['unit_name'],
                'quizzes': item['quizzes'],
                }
            )
            out_dict['changed_units'].append(int(item['unit_number']))
            quiz_table.delete_item(Key={'unit_id': inp_dict['unit_id'], 'unit_number': item['unit_number']})
    out_dict['unit_id'] = inp_dict['unit_id']
    out_dict['removed_unit'] = removed_unit
    json.dump(out_dict, output_json)


def create_unit(input_json, output_json):
    inp_dict = json.load(input_json)
    start_index = inp_dict['unit_number']
    if start_index != 0:
        start_index -= 1
    key_exp = Key('unit_id').eq(inp_dict['curriculum_id'])
    key_exp &= Key('unit_number').gte(start_index)
    response = unit_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='unit_number,description,unit_name,quizzes', 
                               KeyConditionExpression=key_exp)
    out_dict = {'changed_units': []}
    for item in reversed(response['Items']):
        if int(item['unit_number']) < inp_dict['unit_number']:
            continue
        unit_table.put_item(
        Item={
            'unit_id': inp_dict['curriculum_id'],
            'description': item['description'],
            'unit_number': int(item['unit_number']) + 1,
            'unit_name': item['unit_name'],
            'quizzes': item['quizzes'],
        }
        )
        out_dict['changed_units'].append(int(item['unit_number']))
        unit_table.delete_item(Key={'unit_id': inp_dict['curriculum_id'], 'unit_number': item['unit_number']})
    while 'LastEvaluatedKey' in response.keys():
        response = unit_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='unit_number,description,unit_name,quizzes', 
                               KeyConditionExpression=key_exp, ExclusiveStartKey=response['LastEvaluatedKey'])
        for item in reversed(response['Items']):
            if int(item['unit_number']) < inp_dict['unit_number']:
                continue
            unit_table.put_item(
            Item={
                'unit_id': inp_dict['curriculum_id'],
                'description': item['description'],
                'unit_number': int(item['unit_number']) + 1,
                'unit_name': item['unit_name'],
                'quizzes': item['quizzes'],
            }
            )
            out_dict['changed_units'].append(int(item['unit_number']))
            unit_table.delete_item(Key={'unit_id': inp_dict['curriculum_id'], 'unit_number': item['unit_number']})
    unit_table.put_item(
        Item={
            'unit_id': inp_dict['curriculum_id'],
            'description': inp_dict['description'],
            'unit_name': inp_dict['unit_name'],
            'unit_number': inp_dict['unit_number'],
            'quizzes': inp_dict['quizzes'],
        }   
    )
    out_dict['unit_id'] = inp_dict['curriculum_id']
    json.dump(out_dict, output_json)
    
    
def change_unit_order(input_json, output_json):
    inp_dict = json.load(input_json)
    original_pos = inp_dict['unit_number']
    new_pos = inp_dict['new_location']
    if original_pos == new_pos:
        raise Exception('Both positions given are the same')
    k2 = Key('unit_id').eq(inp_dict['unit_id'])
    k2 &= Key('unit_number').eq(original_pos)
    response = unit_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='unit_name,description,quizzes', 
                               KeyConditionExpression=k2)
    while not len(response['Items']) > 0 and 'LastEvaluatedKey' in response.keys():
        response = unit_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='unit_name,description,quizzes', 
                               KeyConditionExpression=k2, ExclusiveStartKey=response['LastEvaluatedKey'])
    removed_unit = response['Items'][0]
    removed_unit['unit_id'] = inp_dict['unit_id']
    removed_unit['unit_number'] = new_pos
    out_dict = {'changed_indexes': []}
    unit_table.delete_item(Key={'unit_id': inp_dict['unit_id'], 'unit_number': original_pos})
    if original_pos > new_pos:
        key_exp = Key('unit_id').eq(inp_dict['unit_id'])
        key_exp &= Key('unit_number').between(new_pos, original_pos - 1)    
        response = unit_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='unit_name,description,quizzes,unit_number', 
                               KeyConditionExpression=key_exp)
        for item in reversed(response['Items']):
            unit_table.put_item(
                Item={
                    'unit_id': inp_dict['unit_id'],
                    'description': item['description'],
                    'unit_number': int(item['unit_number']) + 1,
                    'unit_name': item['unit_name'],
                    'quizzes': item['quizzes'],
                }
            )
            out_dict['changed_indexes'].append(int(item['unit_number']))
        while 'LastEvaluatedKey' in response.keys():
            response = unit_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='unit_name,description,quizzes,unit_number', 
                               KeyConditionExpression=key_exp, ExclusiveStartKey=response['LastEvaluatedKey'])
            for item in reversed(response['Items']):
                unit_table.put_item(
                    Item={
                        'unit_id': inp_dict['unit_id'],
                        'description': item['description'],
                        'unit_number': int(item['unit_number']) + 1,
                        'unit_name': item['unit_name'],
                        'quizzes': item['quizzes'],
                    }
                )
                out_dict['changed_indexes'].append(int(item['unit_number']))
    else:
        key_exp = Key('unit_id').eq(inp_dict['unit_id'])
        key_exp &= Key('unit_number').between(original_pos + 1, new_pos)    
        response = unit_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='unit_name,description,quizzes,unit_number', 
                               KeyConditionExpression=key_exp)
        for item in response['Items']:
            unit_table.put_item(
                Item={
                    'unit_id': inp_dict['unit_id'],
                    'description': item['description'],
                    'unit_number': int(item['unit_number']) - 1,
                    'unit_name': item['unit_name'],
                    'quizzes': item['quizzes'],
                }
            )
            out_dict['changed_indexes'].append(int(item['unit_number']))
        while 'LastEvaluatedKey' in response.keys():
            response = unit_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='unit_name,description,quizzes,unit_number', 
                               KeyConditionExpression=key_exp, ExclusiveStartKey=response['LastEvaluatedKey'])
            for item in response['Items']:
                unit_table.put_item(
                    Item={
                        'unit_id': inp_dict['unit_id'],
                        'description': item['description'],
                        'unit_number': int(item['unit_number']) - 1,
                        'unit_name': item['unit_name'],
                        'quizzes': item['quizzes'],
                    }
                )
                out_dict['changed_indexes'].append(int(item['unit_number']))
    unit_table.put_item(
        Item=removed_unit
    )    
    out_dict['unit_id'] = inp_dict['unit_id']
    json.dump(out_dict, output_json)
    
    
def get_all_lessons(output_json):
    response = db_client.scan(TableName='Lessons', Select='SPECIFIC_ATTRIBUTES', ProjectionExpression='lesson_name,lesson_id,lesson_number')
    out_dict = {}
    for r in response['Items']:
        out_dict[r['lesson_name']['S']] = r['lesson_id']['S']
    while 'LastEvaluatedKey' in response.keys():
        response = db_client.scan(TableName='Lessons', Select='SPECIFIC_ATTRIBUTES', ProjectionExpression='lesson_name,lesson_id,lesson_number', 
                                  ExclusiveStartKey=response['LastEvaluatedKey'])
        for r in response['Items']:
            out_dict[r['lesson_name']['S']] = r['lesson_id']['S']
    json.dump(out_dict, fp=output_json)
    
    
def get_lesson(input_json, output_json):
    inp_dict = json.load(input_json)
    out_dict = {}
    key_exp = Key('lesson_id').eq(inp_dict['lesson_id'])
    key_exp &= Key('lesson_number').eq(inp_dict['lesson_number'])
    response = lesson_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='lesson_desc,video,lesson_name,quizzes,interactives', 
                               KeyConditionExpression=key_exp)
    while not len(response['Items']) > 0 and 'LastEvaluatedKey' in response.keys():
        response = lesson_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='lesson_desc,video,lesson_name,quizzes,interactives', 
                            KeyConditionExpression=key_exp, ExclusiveStartKey=response['LastEvaluatedKey'])
    out_dict = response['Items'][0]
    json.dump(out_dict, output_json)
    
    
def change_lesson_description(input_json, output_json):
    inp_dict = json.load(input_json)
    key_exp = Key('lesson_id').eq(inp_dict['lesson_id'])
    key_exp &= Key('lesson_number').eq(inp_dict['lesson_number'])
    response = lesson_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='lesson_desc', 
                               KeyConditionExpression=key_exp)
    while not len(response['Items']) > 0 and 'LastEvaluatedKey' in response.keys():
        response = lesson_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='lesson_desc', 
                            KeyConditionExpression=key_exp, ExclusiveStartKey=response['LastEvaluatedKey'])
    old_description = response['Items'][0]['lesson_desc']
    lesson_table.update_item(Key={'lesson_id': inp_dict['lesson_id'], 'lesson_number': inp_dict['lesson_number']},
                           UpdateExpression='set lesson_desc = :d',
                           ExpressionAttributeValues={
                               ":d": inp_dict['new_description']
                           })
    out_dict = {}
    out_dict['description'] = inp_dict['new_description']
    out_dict['old_description'] = old_description
    out_dict['lesson_id'] = inp_dict['lesson_id']
    out_dict['lesson_number'] = inp_dict['lesson_number']
    json.dump(out_dict, output_json)
    
    
def change_lesson_name(input_json, output_json):
    inp_dict = json.load(input_json)
    key_exp = Key('lesson_id').eq(inp_dict['lesson_id'])
    key_exp &= Key('lesson_number').eq(inp_dict['lesson_number'])
    response = lesson_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='lesson_name', 
                               KeyConditionExpression=key_exp)
    while not len(response['Items']) > 0 and 'LastEvaluatedKey' in response.keys():
        response = lesson_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='lesson_name', 
                            KeyConditionExpression=key_exp, ExclusiveStartKey=response['LastEvaluatedKey'])
    old_name = response['Items'][0]['lesson_name']
    lesson_table.update_item(Key={'lesson_id': inp_dict['lesson_id'], 'lesson_number': inp_dict['lesson_number']},
                           UpdateExpression='set lesson_name = :d',
                           ExpressionAttributeValues={
                               ":d": inp_dict['new_name']
                           })
    out_dict = {}
    out_dict['name'] = inp_dict['new_name']
    out_dict['old_name'] = old_name
    out_dict['lesson_id'] = inp_dict['lesson_id']
    out_dict['lesson_number'] = inp_dict['lesson_number']
    json.dump(out_dict, output_json)
    
        
def add_lesson_quiz(input_json, output_json):
    inp_dict = json.load(input_json)
    key_exp = Key('lesson_id').eq(inp_dict['lesson_id'])
    key_exp &= Key('lesson_number').eq(inp_dict['lesson_number'])
    curr_list = []
    response = lesson_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='quizzes', 
                               KeyConditionExpression=key_exp)
    while not len(response['Items']) > 0 and 'LastEvaluatedKey' in response.keys():
        response = lesson_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='quizzes', 
                            KeyConditionExpression=key_exp, ExclusiveStartKey=response['LastEvaluatedKey'])
    for item in response['Items']:
        for quiz in item['quizzes']:
            curr_list.append(quiz)
    old_list = curr_list.copy()
    for choice in inp_dict['new_quizzes']:
        curr_list.append(choice)
    lesson_table.update_item(Key={'lesson_id': inp_dict['lesson_id'], 'lesson_number': inp_dict['lesson_number']},
                           UpdateExpression='set quizzes = :qs',
                           ExpressionAttributeValues={
                               ":qs": curr_list
                           })
    out_dict = {}
    out_dict['quizzes'] = curr_list
    out_dict['old_quizzes'] = old_list
    out_dict['lesson_id'] = inp_dict['lesson_id']
    out_dict['lesson_number'] = inp_dict['lesson_number']
    json.dump(out_dict, output_json)
        
        
def add_lesson_interactive(input_json, output_json):
    inp_dict = json.load(input_json)
    key_exp = Key('lesson_id').eq(inp_dict['lesson_id'])
    key_exp &= Key('lesson_number').eq(inp_dict['lesson_number'])
    curr_list = []
    response = lesson_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='interactives', 
                               KeyConditionExpression=key_exp)
    while not len(response['Items']) > 0 and 'LastEvaluatedKey' in response.keys():
        response = lesson_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='interactives', 
                            KeyConditionExpression=key_exp, ExclusiveStartKey=response['LastEvaluatedKey'])
    for item in response['Items']:
        for quiz in item['interactives']:
            curr_list.append(quiz)
    old_list = curr_list.copy()
    for choice in inp_dict['new_interactives']:
        curr_list.append(choice)
    lesson_table.update_item(Key={'lesson_id': inp_dict['lesson_id'], 'lesson_number': inp_dict['lesson_number']},
                           UpdateExpression='set interactives = :qs',
                           ExpressionAttributeValues={
                               ":qs": curr_list
                           })
    out_dict = {}
    out_dict['interactives'] = curr_list
    out_dict['old_interactives'] = old_list
    out_dict['lesson_id'] = inp_dict['lesson_id']
    out_dict['lesson_number'] = inp_dict['lesson_number']
    json.dump(out_dict, output_json)
    
    
def remove_lesson_quiz(input_json, output_json):
    inp_dict = json.load(input_json)
    key_exp = Key('lesson_id').eq(inp_dict['lesson_id'])
    key_exp &= Key('lesson_number').eq(inp_dict['lesson_number'])
    curr_list = []
    response = lesson_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='quizzes', 
                               KeyConditionExpression=key_exp)
    while not len(response['Items']) > 0 and 'LastEvaluatedKey' in response.keys():
        response = lesson_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='quizzes', 
                            KeyConditionExpression=key_exp, ExclusiveStartKey=response['LastEvaluatedKey'])
    for item in response['Items']:
        for quiz in item['quizzes']:
            curr_list.append(quiz)
    old_list = curr_list.copy()
    for choice in inp_dict['remove']:
        curr_list.remove(choice)
    lesson_table.update_item(Key={'lesson_id': inp_dict['lesson_id'], 'lesson_number': inp_dict['lesson_number']},
                           UpdateExpression='set quizzes = :qs',
                           ExpressionAttributeValues={
                               ":qs": curr_list
                           })
    out_dict = {}
    out_dict['quizzes'] = curr_list
    out_dict['old_quizzes'] = old_list
    out_dict['lesson_id'] = inp_dict['lesson_id']
    out_dict['lesson_number'] = inp_dict['lesson_number']
    json.dump(out_dict, output_json)
    
    
def remove_lesson_interactive(input_json, output_json):
    inp_dict = json.load(input_json)
    key_exp = Key('lesson_id').eq(inp_dict['lesson_id'])
    key_exp &= Key('lesson_number').eq(inp_dict['lesson_number'])
    curr_list = []
    response = lesson_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='interactives', 
                               KeyConditionExpression=key_exp)
    while not len(response['Items']) > 0 and 'LastEvaluatedKey' in response.keys():
        response = lesson_table.query(Select="SPECIFIC_ATTRIBUTES", ProjectionExpression='interactives', 
                            KeyConditionExpression=key_exp, ExclusiveStartKey=response['LastEvaluatedKey'])
    for item in response['Items']:
        for quiz in item['interactives']:
            curr_list.append(quiz)
    old_list = curr_list.copy()
    for choice in inp_dict['remove']:
        curr_list.remove(choice)
    lesson_table.update_item(Key={'lesson_id': inp_dict['lesson_id'], 'lesson_number': inp_dict['lesson_number']},
                           UpdateExpression='set interactives = :qs',
                           ExpressionAttributeValues={
                               ":qs": curr_list
                           })
    out_dict = {}
    out_dict['interactives'] = curr_list
    out_dict['old_interactives'] = old_list
    out_dict['lesson_id'] = inp_dict['lesson_id']
    out_dict['lesson_number'] = inp_dict['lesson_number']
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
remove_question_input = open('remove_question_data.json')
change_question_order_input = open('change_question_order_data.json')
create_curriculum_input = open('create_curriculum_data.json')
change_curriculum_name_input = open('change_curriculum_name_data.json')
change_curriculum_description_input = open('change_curriculum_description_data.json')
change_image_input = open('change_image_data.json')
get_curriculum_input = open('get_curriculum_data.json')
get_unit_input = open('get_unit_data.json')
change_unit_description_input = open('change_unit_description_data.json')
add_unit_quiz_input = open('add_unit_quiz_data.json')
remove_unit_quiz_input = open('remove_unit_quiz_data.json')
change_unit_name_input = open('change_unit_name_data.json')
delete_unit_input = open('delete_unit_data.json')
create_unit_input = open('create_unit_data.json')
change_unit_order_input = open('change_unit_order_data.json')
get_lesson_input = open('get_lesson_data.json')
change_lesson_description_input = open('change_lesson_description_data.json')
change_lesson_name_input = open('change_lesson_name_data.json')
add_lesson_quiz_input = open('add_lesson_quiz_data.json')
add_lesson_interactive_input = open('add_lesson_interactive_data.json')
remove_lesson_quiz_input = open('remove_lesson_quiz_data.json')
remove_lesson_interactive_input = open('remove_lesson_interactive_data.json')
# print_table('Units')
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
# remove_question(remove_question_input, output_file)
# change_question_order(change_question_order_input, output_file)
# create_curriculum(create_curriculum_input, output_file)
# get_all_curriculums(output_file)
# change_curriculum_name(change_curriculum_name_input, output_file)
# change_curriculum_description(change_curriculum_description_input, output_file)
# change_image(change_image_input, output_file)
# get_curriculum(get_curriculum_input, output_file)
# get_unit(get_unit_input, output_file)
# change_unit_description(change_unit_description_input, output_file)
# add_unit_quiz(add_unit_quiz_input, output_file)
# remove_unit_quiz(remove_unit_quiz_input, output_file)
# change_unit_name(change_unit_name_input, output_file)
# remove_unit(delete_unit_input, output_file)
# create_unit(create_unit_input, output_file)
# change_unit_order(change_unit_order_input, output_file)
# get_all_lessons(output_file)
# get_lesson(get_lesson_input, output_file)
# change_lesson_description(change_lesson_description_input, output_file)
# change_lesson_name(change_lesson_name_input, output_file)
# add_lesson_quiz(add_lesson_quiz_input, output_file)
# add_lesson_interactive(add_lesson_interactive_input, output_file)
# remove_lesson_quiz(remove_lesson_quiz_input, output_file)
remove_lesson_interactive(remove_lesson_interactive_input, output_file)
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
remove_question_input.close()
change_question_order_input.close()
create_curriculum_input.close()
change_curriculum_name_input.close()
change_curriculum_description_input.close()
change_image_input.close()
get_curriculum_input.close()
get_unit_input.close()
change_unit_description_input.close()
add_unit_quiz_input.close()
remove_unit_quiz_input.close()
change_unit_name_input.close()
delete_unit_input.close()
create_unit_input.close()
change_unit_order_input.close()
get_lesson_input.close()
change_lesson_description_input.close()
change_lesson_name_input.close()
add_lesson_quiz_input.close()
add_lesson_interactive_input.close()
remove_lesson_quiz_input.close()
remove_lesson_interactive_input.close()