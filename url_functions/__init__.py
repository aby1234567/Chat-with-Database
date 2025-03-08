from flask import request,jsonify
import os
from gevent import time
from utils import chat_models,extract_sql_query,prompts
from utils.schema import DB_SCHEMA
from dotenv import load_dotenv
from connection_handlers import ConnectionPool
# from connection_handlers import connector

load_dotenv()

connection_manager=ConnectionPool()
llm=chat_models(provider='vertexai',
                model=None,
                temperature=0,
                top_p=0.95,
                max_tokens=1500,
                region=None)
    
def generate_token():
    from flask_jwt_extended import create_access_token
    username = request.json['user']
    #print(username)
    password = request.json['password']
    #print(password)
    #print(config['USER'], config['PASSWORD'])
    if username != os.getenv('USER') or password != os.getenv('PASSWORD'):
        return jsonify({"response": "Bad username or password"}), 400

    access_token = create_access_token(identity=username)
    return jsonify(access_token=access_token)
    
def documentation(endpoint=None):
    authentication=str(request.authorization)
    if authentication!=f'Bearer - {os.getenv("JWT_SECRET_KEY")}':
        return jsonify({'response':'Authentication failed'
                            }),401
    if not endpoint:
        return jsonify({"response": """Available endpoints\n1. llm_response\n2. close_idle_connections"""}), 200
    if endpoint=='llm_response':
        response=({"response":{'user_query':'pass the question asked by the user here',
                'previous_answers':"pass the answers to the previous questions here",
                'set_cache':'enable or disable cache, caching bit has been disabled for now, as its not required as of yet'}},200)
    
    # return response,200
    else:
        response=({"response": "No such endpoint found"},400)
    
    return response

def get_llm_response():

    if request.content_type!='application/json':
        return jsonify({"response": "Content-Type must be application/json"}), 400   
    json=request.get_json()   
    try:
        query= json['user_query']
    except Exception as e:
        return jsonify({"response": f"{e} is missing in body"}), 400
    
    answers=json.get('previous_answers')

    try:
        answer_length=len(answers)
    except:
        answer_length=0
    query_length=len(query)

    if answer_length!=query_length-1:
        return jsonify({"response": f"There are {answer_length} answers and {query_length} questions. The number of answers should always be 1 less than questions"}), 400    

    # user_query=query

    # r_provider = json.get('provider','vertexai')
    # r_region=json.get('region')
    # if r_provider=='bedrock':
    #     if not r_region:
    #         r_region='india'
    # r_temperature = json.get('temperature',0)
    # r_top_p = json.get('top_p',0.95)
    # r_max_tokens= json.get('max_tokens',1500)
        
    # r_model=json.get('model')  

    # if r_provider!=provider or r_region!=region or r_temperature!=temperature or r_top_p!=top_p or r_max_tokens!=r_max_tokens or r_model!=model:
    #     provider=r_provider
    #     region=r_region
    #     temperature=r_temperature
    #     max_tokens=r_max_tokens
    #     top_p=r_top_p
    #     llm=chat_models(provider,model,temperature,top_p,max_tokens,region)    
    
    model=llm.model

    completion_tokens,prompt_tokens,total_tokens,llm_resp_time,query_execution_time=0,0,0,0,0  
       
    
    # is_related_status=False
    # if previous_question_list:
    #     #print(previous_question_list)
    #     new_query,resp_time,token_usage3=check_if_related_to_previous_question(time,prompts.CONVERSATION_HISTORY_PROMPT.format(previous_question_list),query,llm,model,region)
    #     if new_query.strip().lower()!='no':
    #         query=new_query
    #         is_related_status=True
    #     llm_resp_time+=resp_time
    #     completion_tokens+=token_usage3['completion_tokens']
    #     prompt_tokens+=token_usage3['prompt_tokens']
    #     total_tokens+=token_usage3['total_tokens']

    error=None
    #error_query=None
    sql_error_retry=0
    while sql_error_retry<=2:
        if not error:
            #prompt = ChatPromptTemplate.from_template(template=prompts.GENERATE_SQL_PROMPT)
            #chain=prompt|llm
            sql_gen_start=time.time()
            prompt=prompts.GENERATE_SQL_PROMPT.format(DB_SCHEMA)
            response=llm.invoke(prompt,query,answers)
            sql_gen_end=time.time()
        
        else:

            #prompt = ChatPromptTemplate.from_template(template=prompts.SQL_CORRECTION_PROMPT)
            #chain=prompt|llm
            sql_gen_start=time.time()
            user_message=f"'SQL' : {error_query}, 'error' : {error}"
            response=llm.invoke(prompts.SQL_CORRECTION_PROMPT.format(DB_SCHEMA),[user_message],None)
            sql_gen_end=time.time()

        #print('Response : ',response.content)
        response_content=response.content.lower()
        token_usage=response.response_metadata        
        if response_content == 'no':
            import random
            questions = [
    "It appears you posed a question that I found confusing or unrelated; could you please rephrase it?",
    "I think you've asked something I don't quite grasp or that doesn't seem relevant; can you restate your question?",
    "It seems you've raised a question that I can't comprehend or that's off-topic; could you clarify it?",
    "You seem to have asked something I didn’t understand or that seems irrelevant; could you reword it?",
    "I feel like your question was unclear or not applicable; could you please ask it again differently?",
    "It looks like you've asked something I can't quite grasp or that isn't relevant; can you clarify?",
    "I think your question may be confusing or irrelevant; could you rephrase it for clarity?",
    "It seems you've posed a question that's unclear to me or not pertinent; could you please reword it?",
    "I feel like the question you asked is either confusing or off-topic; could you please clarify it?",
    "It appears that your question is difficult for me to understand or not applicable; could you clarify?",
    "It looks like you've asked something I can't fully comprehend or that seems irrelevant; can you restate it?",
    "I believe your question is either unclear or not relevant; could you provide more context?",
    "It seems you raised a question that I couldn’t comprehend or that feels unrelated; can you clarify?",
    "I think your question may be confusing or off-topic; could you ask it again in a different way?",
    "It appears that the question you've posed is confusing to me or misses the point; could you clarify?",
    "I think your question might be hard to grasp or off-topic; can you restate it for me?",
    "It seems you've brought up a question that's hard to understand or unrelated; can you clarify it?",
    "I feel your question might not be entirely clear or relevant; could you please rephrase it?",
    "It looks like you've asked something that’s confusing or off-topic; can you clarify it for better understanding?",
    "I believe your question might not be straightforward or relevant; could you provide a clearer version?",
    "It seems you posed something that’s difficult for me to follow or feels irrelevant; can you clarify?",
    "I think your question could be confusing or not applicable; can you please restate it?",
    "It appears that your question is hard for me to grasp or not pertinent; could you clarify?",
    "It looks like you've asked something that’s difficult to understand or that feels unrelated; could you clarify?",
    "I think your question may not be clear or relevant; could you please provide further explanation?",
    "It seems you've raised a question that I find confusing or off-topic; could you clarify it?",
    "I feel like your question is either unclear or not directly relevant; could you reword it?",
    "It seems you've asked something I can’t quite follow or that feels unrelated; can you clarify it?",
    "I believe your question might not be clear or off-topic; could you provide a clearer version?",
    "It appears that the question you asked is difficult to understand or irrelevant; could you clarify?",
    "It seems you posed a question that is hard to understand or feels unrelated; can you clarify it?",
    "I think your question may be confusing or irrelevant; could you please clarify it?",
    "It looks like you've raised a question I don’t fully understand or that isn’t relevant; could you clarify?",
    "I feel your question could be unclear or off-topic; could you provide a clearer version?",
    "It appears that your question is hard for me to follow or not applicable; could you clarify?",
    "I think your question might be confusing or off-topic; could you rephrase it for better understanding?",
    "It seems you posed a question that’s difficult for me to follow or feels unrelated; can you clarify?",
    "I believe your question is either unclear or not relevant; could you please ask it in another way?",
    "It looks like you asked something that’s hard for me to comprehend or feels off-topic; can you clarify?",
    "I think your question could be confusing or not applicable; can you provide further context?",
    "It seems you've raised a question that I find confusing or not pertinent; could you restate it?",
    "I feel your question may not be clear or relevant; could you please rephrase it?",
    "It looks like you posed a question that is confusing or not applicable; can you clarify it?",
    "I believe your question might be difficult to understand or not entirely relevant; could you clarify?",
    "It seems you've asked something that’s hard for me to follow or feels off-topic; can you clarify?",
    "I think your question could be unclear or not applicable; can you provide a clearer version?",
    "It appears that your question is hard for me to grasp or not relevant; could you clarify?",
    "It looks like you've posed a question that's confusing or not directly relevant; can you clarify?",
    "I feel like your question may not be entirely clear or relevant; could you please clarify?",
    "It seems you raised a question that is hard to follow or feels irrelevant; can you restate it?",
    "I believe your question is either unclear or not applicable; could you please clarify?",
    "It appears that your question is confusing or not pertinent; could you restate it for better understanding?",
    "It looks like you've asked something that’s difficult for me to comprehend or feels off-topic; can you clarify?",
    "I think your question might not be clear or relevant; could you provide further details?",
    "It seems you've posed a question that I find unclear or not applicable; can you clarify?",
    "I feel like your question is either unclear or not relevant; could you please rephrase it?",
    "It looks like you’ve raised something I can’t fully comprehend or that’s off-topic; could you clarify?",
    "I believe your question might not be straightforward or applicable; could you provide a clearer version?",
    "It seems you've brought up a question that is hard to understand or irrelevant; could you clarify it?",
    "I think your question could be confusing or not relevant; could you please clarify it?",
    "It appears that your question is difficult for me to grasp or unrelated; could you clarify?",
    "It seems you have posed a question that I find unclear or not pertinent; could you rephrase it?",
    "I think your question may be confusing or not directly relevant; could you clarify?",
    "It looks like you raised a question I can’t fully understand or that isn’t applicable; could you clarify?",
    "I believe your question might not be straightforward or relevant; could you please provide more details?",
    "It seems you've asked something that’s hard for me to follow or that feels irrelevant; can you clarify?",
    "I feel like your question may not be clear or relevant; could you please restate it?",
    "It appears that your question is hard for me to grasp or not relevant; could you clarify?",
    "It looks like you've posed a question that’s confusing or not applicable; can you clarify?",
    "I believe your question might not be clear or off-topic; could you please restate it?",
    "I think your question may not be easy to understand; could you clarify it for me?",
    "It seems like your question is confusing or not applicable; can you provide more context?"
]

            return jsonify({'response':{'message':random.choice(questions),
                                        'logs':{'error':'improper_user_request',
                                                'type':'improper_user_request',    
                                                'query_feedback_loop_exec_count':sql_error_retry,
                                                'completion_tokens':token_usage['completion_tokens'],
                                                'prompt_tokens':token_usage['prompt_tokens'],
                                                'total_tokens':token_usage['total_tokens']}
                                        }
                            }),411
        
        elif response_content=='wrong':
            return jsonify({'response':{'message':'I can only answer questions for your restaurant, not other restaurants. Please reframe the question accordingly.',
                                        'logs':{'error':'improper_user_request',
                                                'type':'improper_user_request',    
                                                'query_feedback_loop_exec_count':sql_error_retry,
                                                'completion_tokens':token_usage['completion_tokens'],
                                                'prompt_tokens':token_usage['prompt_tokens'],
                                                'total_tokens':token_usage['total_tokens']}
                                        }
                            }),411            
        
        restaurant_handle_in_sql=extract_sql_query(response.content).replace("\\","")
        # print(restaurant_handle_in_sql)
        # sql=restaurant_handle_in_sql
        # for i in range(len(sql)):
        #     if i+4<len(sql):
        #         if sql[i] + sql[i+1] + sql[i+2] + sql[i+3] + sql[i+4] in ('where','WHERE'):
        #             new_sql=sql[i:]
        #             sql=sql[:i+5]+f" city='{city}' and "+sql[i+6:]
        #             #print(new_sql)
        #             for j in range(len(new_sql)):
        #                 if j+10<len(new_sql):
        #                     if new_sql[j] + new_sql[j+1] + new_sql[j+2] + new_sql[j+3] + new_sql[j+4] + new_sql[j+5] + new_sql[j+6] + new_sql[j+7] + new_sql[j+8] + new_sql[j+9] + new_sql[j+10] == 'restaurant_':
        #                         pos=sql.find('=',i+j+17)
        #                         sql=sql[:pos]+'!='+sql[pos+1:]
        #                         break 

        # restaurant_handle_notin_sql=sql                        
        #print(restaurant_handle_notin_sql)

        completion_tokens+=token_usage['completion_tokens']
        prompt_tokens+=token_usage['prompt_tokens']
        total_tokens+=token_usage['total_tokens']
        llm_resp_time+=(sql_gen_end-sql_gen_start)

        # connection=ConnectionPool()          
        db_connect=0
        while db_connect<=2:
            try:
                conn=connection_manager.get_connection()
                cursor=conn[0].cursor()
                conn[1]=sql_exec_start=time.time()
                cursor.execute(restaurant_handle_in_sql)
                result=cursor.fetchall()
                #print('in exec end')
                column_names = [desc[0] for desc in cursor.description]
                #print('notin exec start')
                # cursor.execute(restaurant_handle_notin_sql)
                # result2=cursor.fetchall()
                #print('not in exec end')
                # column_names2 = [desc[0] for desc in cursor.description]
                sql_exec_end=time.time()
                dicti={}
                for i in range(len(column_names)):
                    dicti[column_names[i]]=[]
                    for j in range(len(result)):
                        dicti[column_names[i]].append(result[j][i])
                # dicti2={}
                # for i in range(len(column_names2)):
                #     dicti2[column_names2[i]]=[]
                #     for j in range(len(result2)):
                #         dicti2[column_names2[i]].append(result2[j][i])
                #print(dicti)
                cursor.close()
                connection_manager.return_connection(conn)
                query_execution_time+=(sql_exec_end-sql_exec_start)

                return jsonify({'response':{'data':{'restaurant_handle_equal_to':dicti,
                                                    'restaurant_handle_not_equal_to':None},
                                            'logs':{    'query_feedback_loop_exec_count':sql_error_retry,
                                                        'completion_tokens':completion_tokens,
                                                        'prompt_tokens':prompt_tokens,
                                                        'total_tokens':total_tokens,
                                                        'llm_resp_time_in_s':round(llm_resp_time,2),
                                                        'query_execution_time_in_s':round(query_execution_time,2),
                                                        'generated_queries':{'restaurant_handle_equal_to':restaurant_handle_in_sql,
                                                                            'restaurant_handle_not_equal_to':None},
                                                        'model':model}}}),200 
        
            except connection_manager.syntax_error as e:
                sql_exec_end=time.time()
                query_execution_time+=(sql_exec_end-sql_exec_start)
                error=str(e)
                error_query=restaurant_handle_in_sql
                sql_error_retry+=1
                print('syntax error :', e)
                if sql_error_retry>=3:
                    return jsonify({'response':{'message':f'I could not process your question, please try reframing the question',
                                                'logs':{'error':error,
                                                        'type':'syntax_error',
                                                        'query_feedback_loop_exec_count':sql_error_retry,
                                                        'completion_tokens':completion_tokens,
                                                        'prompt_tokens':prompt_tokens,
                                                        'total_tokens':total_tokens,
                                                        'llm_resp_time_in_s':round(llm_resp_time,2),
                                                        'query_execution_time_in_s':round(query_execution_time,2),
                                                        'generated_queries':{'restaurant_handle_equal_to':restaurant_handle_in_sql,
                                                                            'restaurant_handle_not_equal_to':None},
                                                        'model':model}}}),412
                break
            
            except connection_manager.operational_error as e:
                db_connect+=1
                print("retrying count due to ",e,' : ',db_connect)
                if db_connect>2:
                    return jsonify({'response':{'message':f'I could not process your question at the moment, please try again after sometime',
                                                'logs':{'error':str(e),
                                                        'type':'redshift_operational_error',
                                                        'query_feedback_loop_exec_count':sql_error_retry,
                                                        'completion_tokens':completion_tokens,
                                                        'prompt_tokens':prompt_tokens,
                                                        'total_tokens':total_tokens,
                                                        'llm_resp_time_in_s':round(llm_resp_time,2),
                                                        'query_execution_time_in_s':round(query_execution_time,2),
                                                        'generated_queries':{'restaurant_handle_equal_to':restaurant_handle_in_sql,
                                                                            'restaurant_handle_not_equal_to':None},
                                                        'model':model}}}),413
        
            except connection_manager.database_error as e:
                db_connect+=1
                print("retrying count due to ",e,' : ',db_connect)
                if db_connect>2:
                    return jsonify({'response':{'message':f'I could not process your question, please try reframing the question',
                                                'logs':{'error':str(e),
                                                        'type':'redshift_database_error',
                                                        'query_feedback_loop_exec_count':sql_error_retry,
                                                        'completion_tokens':completion_tokens,
                                                        'prompt_tokens':prompt_tokens,
                                                        'total_tokens':total_tokens,
                                                        'llm_resp_time_in_s':round(llm_resp_time,2),
                                                        'query_execution_time_in_s':round(query_execution_time,2),
                                                        'generated_queries':{'restaurant_handle_equal_to':restaurant_handle_in_sql,
                                                                            'restaurant_handle_not_equal_to':None},
                                                        'model':model}}}),414
            
            except TimeoutError as e:
                db_connect+=1
                print("retrying count due to ",e,' : ',db_connect)
                if db_connect>2:
                    return jsonify({'response':{'message':f'I could not process your question, please try reframing the question',
                                                'logs':{'error':str(e),
                                                        'type':'redshift_timeout_error',
                                                        'query_feedback_loop_exec_count':sql_error_retry,
                                                        'completion_tokens':completion_tokens,
                                                        'prompt_tokens':prompt_tokens,
                                                        'total_tokens':total_tokens,
                                                        'llm_resp_time_in_s':round(llm_resp_time,2),
                                                        'query_execution_time_in_s':round(query_execution_time,2),
                                                        'generated_queries':{'restaurant_handle_equal_to':restaurant_handle_in_sql,
                                                                            'restaurant_handle_not_equal_to':None},
                                                        'model':model}}}),415


        