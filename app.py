from flask import Flask,jsonify,request
from os import getenv
import url_functions as uf
from functools import wraps
# from flask_caching import Cache
# from gevent import spawn,sleep
#from flask_jwt_extended import JWTManager, jwt_required

app=Flask(__name__)

# Flask-Caching related configs
# config = {          
#     "CACHE_TYPE": "SimpleCache",  
#     "CACHE_DEFAULT_TIMEOUT": 300
# }

# app.config.from_mapping(config)
# cache = Cache(app)

#jwt = JWTManager(app)

#app.add_url_rule('/generate_token','token_generator',generate_token,methods=['POST'])

## Connection manager

def cache_and_authentication_handler(func,key_prefix=None, timeout=300,*args,**kwargs):
    
    """First point of contact function to handle API access authentication and then to 
    cache the outputs of API responses. The caching code has been commented out as it's
    not needed as of now.
    
    Params :

        func : pass the function that needs to be called
        
        key_prefix : unique identifier of the function
        
        timeout : cache timeout

    """

    wraps(func)
    #@jwt_required()
    def wrapped_func():
        authentication=str(request.authorization)
        if authentication!=f'Bearer - {getenv("JWT_SECRET_KEY")}':
            return jsonify({'response':'Authentication failed'
                                }),401
        
        # json=request.get_json()
        # previous_questions=json.get('previous_questions',[])
        # user_question=json.get('user_query')
        # restaurant_handle=json.get('restaurant_handle')
        # set_cache=json.get('set_cache',True)
        # cache_key = f"{key_prefix}:{user_question},{restaurant_handle}"
        try:
            # Check if the response is already cached
            # cached_response = cache.get(cache_key)
            # if cached_response:
            #     return cached_response

            # Call the wrapped function
            response,status = func(*args,**kwargs)

            # Check the status code of the response   
            # if status == 200 and set_cache:
            #     length=len(user_question)
            #     if length>1:
            #         if user_question[length]!=user_question[length-1]:
            #             cache.set(cache_key,response, timeout=timeout)
            #     else:
            #         cache.set(cache_key,response, timeout=timeout)
            
            return response,status

        except Exception as e:  
            import traceback
            tb=traceback.format_exc()
            return jsonify({'response':{'message':'An error has occurred while processing your request',
                                        'logs':{'error':tb,
                                                'type':str(e)}
                                        }
                            }),500

    return wrapped_func

# CRM API
llm_response_cache=cache_and_authentication_handler(uf.get_llm_response)
app.add_url_rule('/llm_response','llm_response',llm_response_cache,methods=['POST'])

# Available enpoints API
app.add_url_rule('/documentation','llm_response_params_details',uf.documentation,methods=['POST'])

# Documentation of available API's endpoint
app.add_url_rule('/documentation/<endpoint>','llm_response_params_details',uf.documentation,methods=['POST'])

# Close idle DBMS connections
# close_idle_conn=cache_and_authentication_handler(uf.connection_manager.close_idle_connections)
def null_return():
    return {'response':'Done'}
app.add_url_rule('/close_idle_connections','cleanup_connections',null_return,methods=['POST'])

