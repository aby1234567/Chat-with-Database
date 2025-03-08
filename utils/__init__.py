from os import getenv
from requests import post
import json
import re
import jwt
from dotenv import load_dotenv
from gevent import spawn_later,time

load_dotenv()

extract_sql_query = lambda text: re.search(
    r'\b(WITH|SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|TRUNCATE)\b.*?(?:;|$)',
    text,
    flags=re.IGNORECASE | re.DOTALL
).group(0).strip('` \n')

def find_occurrences(string, char):
    indices = []
    start = 0
    while True:
        start = string.find(char, start)
        if start == -1:
            break
        indices.append(start)
        start += 1 
    return indices


# def check_if_related_to_previous_question(time,prompttemplate,query,llm,model,region):

#     #prompt = ChatPromptTemplate.from_template(template=prompttemplate)
#     #chain=prompt|llm
#     time_start=time.time()
#     response=llm.invoke(model,prompttemplate,[query],None,200,0,0.1,region_name=region)
#     time_end=time.time()
#     token_usage=response.response_metadata
#     response=response.content
#     #print('New query : ',response)
#     total_time=time_end-time_start
#     return response,total_time,token_usage

class chat_models:

    """Custom class to handle connection to available LLM providers
    Params:
        provider : groq, vertexai, bedrock, openai
        model : the name of the LLM model provided by the LLM provider, call chat_models.model to check the default selected model and chat_models.available_models to check the available models provided by the provider
        temperature: Controls the randomness of the LLM model,
        top_p: Controls the pool size of the words to be selected whose cumulative probabilities meet the given top_p
        max_tokens : The maximum number of token to be used for generating responses
        region (valid only for bedrock) : india, usa.
        get_models_only : will initialize the class with the available models only, set True only for the documentation URL, if used with True for calling API's, etc, will throw unexpected errors"""
    
    def __init__(self,provider:str,model:str=None,temperature:float=0.0,top_p:float=0.95,max_tokens:int=1000,region:str=None,get_models_only=False):
        self.__provider=provider
        self.__temperature=temperature
        self.__top_p=top_p
        self.__max_tokens=max_tokens
        
        if self.__provider=='groq':
            self.available_models=['llama-3.1-70b-versatile','llama3-70b-8192','llama3-8b-8192','mixtral-8x7b-32768','gemma-7b-it']
            if not get_models_only:
                if model:
                    if model not in self.available_models:
                        raise Exception(f"Invalid model, models available are {self.available_models}")                
                    self.model=model
                else:
                    self.model='llama-3.1-70b-versatile'
                self.__url=getenv('GROQ_URL')
                self.__token=getenv('GROQ_API_KEY')
            
        elif self.__provider=='vertexai':
            
            self.available_models=['gemini-1.5-flash-001','gemini-flash-experimental','gemini-pro-experimental','gemini-1.5-pro-001','gemini-1.0-pro-002','gemini-1.0-pro-001']                     
            if not get_models_only:
                if model:
                    if model not in self.available_models:
                        raise Exception(f"Invalid model, models available are {self.available_models}")
                    self.model=model
                else:
                    self.model='gemini-1.5-flash-001'
                self.__url=getenv('GOOGLE_VERTEXAI_INFERENCE_URL')
                with open(getenv('GOOGLE_APPLICATION_CREDENTIALS')) as f:
                    self.__service_account_info = json.load(f)
                
                self.__token_refreshal=False
                self.__get_token()

        elif self.__provider=='bedrock':

            if region=='india' or not region:
                self.__region=getenv("AWS_INDIA_REGION")
            elif region=='usa':
                self.__region=getenv("AWS_USA_REGION")
            else:
                raise Exception('Invalid region')
            
            if region=='india' or not region:
                self.available_models=['anthropic.claude-3-sonnet-20240229-v1:0',
                                'anthropic.claude-3-haiku-20240307-v1:0','meta.llama3-8b-instruct-v1:0',
                                'meta.llama3-70b-instruct-v1:0']
                if model:
                    if model not in self.available_models:
                        raise Exception(f"Invalid model, models available are {self.available_models}")
                    self.model=model
                else:
                    self.model='meta.llama3-70b-instruct-v1:0'
            elif region=='usa':
                self.available_models=['anthropic.claude-3-sonnet-20240229-v1:0',
                                'anthropic.claude-3-haiku-20240307-v1:0','anthropic.claude-v2',
                                'anthropic.claude-v2:1','anthropic.claude-instant-v1',
                                'meta.llama2-13b-chat-v1','meta.llama2-70b-chat-v1','meta.llama3-8b-instruct-v1:0',
                                'meta.llama3-70b-instruct-v1:0']   
                if model:
                    if model not in self.available_models:
                        raise Exception(f"Invalid model, models available are {self.available_models}")
                    self.model=model
                else:
                    self.model='meta.llama3-70b-instruct-v1:0'  
            if not get_models_only:
                self.__url=getenv('BEDROCK_URL')
                self.__access_key_id=getenv('aws_access_key_id')
                self.__secret_key=getenv('aws_secret_access_key')          
        
        elif self.__provider=='openai':
            self.available_models=['gpt-4o','gpt-3.5-turbo-0125','gpt-4-turbo','gpt-4','gpt-4o-mini']
            if not get_models_only:
                if model:
                    if model not in self.available_models:
                        raise Exception(f"Invalid model, models available are {self.available_models}")
                    self.model=model
                else:
                    self.model='gpt-4o-mini'
                self.__url = getenv('OPENAI_URL')
                self.__token = getenv('OPENAI_API_KEY')
            
    def __get_token(self):

        """Creates token for accessing google's apis with an hour's validity.
        Returns the token along with it's expiring time in unixtime."""

        self.__token_refreshal=True
        # print('Token refreshing : ',time.time())

        token_uri = getenv('GOOGLE_TOKEN_URI')
        iat = int(time.time())
        exp = iat + 3600 # Token expires in 1 hour

        payload = {
            "iss": self.__service_account_info['client_email'],
            "scope": getenv('GOOGLE_SCOPE_URI'),
            "aud": token_uri,
            "exp": exp,
            "iat": iat
        }
        signed_jwt = jwt.encode(payload, self.__service_account_info['private_key'], algorithm='RS256')
        try:
            response = post(
                token_uri,
                headers={'Content-Type': 'application/x-www-form-urlencoded'},
                data={
                    'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
                    'assertion': signed_jwt
                }
            )
        except Exception as e:
            raise Exception(f'Could not get google access token due to {e}')
        
        access_token = response.json()

        # time.sleep(5)

        self.__token=access_token['access_token']

        self.__token_refreshal=False

        # print('token refresh complete : ',time.time())

        spawn_later(exp-5-time.time(), self.__get_token)

        # return access_token['access_token'],iat
    
    def __get_aws_headers(self,model:str,payload:dict):
        
        """Creates headers with the AWS4 signature for requests to AWS resources
        Params:
            model : Bedrock models
            payload : the dict of user and assistant messaged to be sent"""

        from  hashlib import sha256
        from hmac import new
        from datetime import timezone,datetime

        request_parameters_json=json.dumps(payload)
        
        def sign(key, msg):
            return new(key, msg.encode('utf-8'), sha256).digest()

        def getSignatureKey(key, dateStamp, regionName, serviceName):
            kDate = sign(('AWS4' + key).encode('utf-8'), dateStamp)
            kRegion = sign(kDate, regionName)
            kService = sign(kRegion, serviceName)
            kSigning = sign(kService, 'aws4_request')
            return kSigning

        # Use the recommended approach for UTC
        t = datetime.now(timezone.utc)
        amz_date = t.strftime('%Y%m%dT%H%M%SZ')
        datestamp = t.strftime('%Y%m%d')  # Date in YYYYMMDD format

        # Create Canonical Request
        canonical_uri = f"/model/{model.replace(':','%253A')}/converse"
        canonical_querystring = ''
        canonical_headers = f'content-type:application/json\nhost:bedrock-runtime.{self.__region}.amazonaws.com\nx-amz-date:{amz_date}\n'
        signed_headers = 'content-type;host;x-amz-date'
        payload_hash = sha256(request_parameters_json.encode('utf-8')).hexdigest()
        canonical_request = f"POST\n{canonical_uri}\n{canonical_querystring}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
        # print(canonical_request,'\n')

        # Create String to Sign
        credential_scope = f"{datestamp}/{self.__region}/bedrock/aws4_request"
        canonical_request_hash = sha256(canonical_request.encode('utf-8')).hexdigest()
        string_to_sign = f"AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{canonical_request_hash}"
        # print(string_to_sign,'\n')

        # Calculate the Signature
        signing_key = getSignatureKey(self.__secret_key, datestamp, self.__region, 'bedrock')
        signature = new(signing_key, string_to_sign.encode('utf-8'), sha256).hexdigest()

        # Construct the Authorization Header
        authorization_header = (
            f"AWS4-HMAC-SHA256 "
            f"Credential={self.__access_key_id}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, "
            f"Signature={signature}"
        ).strip()

        # print(authorization_header)

        # Prepare headers
        headers = {
            'Content-Type': 'application/json',  # This indicates the body is JSON
            'X-Amz-Date': amz_date,
            'Authorization': authorization_header
        }

        return headers           

    
    class __chat_model_response:

        """Response object class that takes the LLM response as params and parses it
        Params:
            provider : groq, vertexai, bedrock, openai
            model_response : the response returned by the requests.post() method"""
        
        def __init__(self,provider,model_response):
            self.status_code=model_response.status_code
            if self.status_code==200:
                self.json=model_response.json()                
                self.headers=model_response.headers
                self.full_response=model_response
                if provider=='vertexai':
                    self.content=self.json['candidates'][0]['content']['parts'][0]['text'].strip()
                    self.response_metadata={'prompt_tokens':self.json['usageMetadata']['promptTokenCount'],
                                        'completion_tokens':self.json['usageMetadata']['candidatesTokenCount'],
                                        'total_tokens':self.json['usageMetadata']['totalTokenCount']}
                elif provider=='bedrock':
                    self.content=self.json['output']['message']['content'][0]['text'].strip()
                    self.response_metadata={'prompt_tokens':self.json['usage']['inputTokens'],
                                        'completion_tokens':self.json['usage']['outputTokens'],
                                        'total_tokens':self.json['usage']['totalTokens']}
                else:
                    self.content=self.json['choices'][0]['message']['content'].strip()
                    self.response_metadata=self.json['usage']
            else:
                error=model_response.json()
                raise Exception(error) 
                       
    def __message_generator(self,system_message,user_message,assistant_message):

        """Formats the system, user and assistant messages passed to the invoke() method
        to the formats accepted by the LLM providers
        """
        messages=[]
        if assistant_message:
            # if len(user_message)!=len(assistant_message)+1:
            #     raise Exception ('The number of assistant_message should always be 1 less than number of user_message')
            if self.__provider=='vertexai':
                for i in range(len(assistant_message)):
                    messages.append({
                                    "role": "user",
                                    "parts": [
                                        {
                                            "text": user_message[i]
                                        },
                                    ]
                                })
                    messages.append({
                                    "role": "user",
                                    "parts": [
                                        {
                                            "text": assistant_message[i]
                                        },
                                    ]
                                })
                messages.append({
                                    "role": "user",
                                    "parts": [
                                        {
                                            "text": user_message[i+1]
                                        },
                                    ]
                                })
            elif self.__provider=='bedrock':
                for i in range(len(assistant_message)):
                    messages.append({
                            "role": "user",
                            "content": [{'text':user_message[i]}]
                        }) 
                    messages.append({
                            "role": "assistant",
                            "content": [{'text':assistant_message[i]}]
                        }) 
                messages.append({
                            "role": "user",
                            "content": [{'text':user_message[i+1]}]
                        })
            else:
                
                messages.append({"role":'system',
                    "content":system_message})
        
                for i in range(len(assistant_message)):
                    messages.append({
                            "role": "user",
                            "content": user_message[i]
                        }) 
                    messages.append({
                            "role": "assistant",
                            "content": assistant_message[i]
                        }) 
                messages.append({
                            "role": "user",
                            "content": user_message[i+1]
                        })
                    
        else:
            # if len(user_message)>1:
            #     raise Exception ('The number of assistant_message should always be 1 less than number of user_message')
            
            if self.__provider=='vertexai':
                messages.append({
                                    "role": "user",
                                    "parts": [
                                        {
                                            "text": user_message[0]
                                        },
                                    ]
                                })              
            elif self.__provider=='bedrock':            
                messages.append({
                            "role": "user",
                            "content": [{'text':user_message[0]}]
                        })
            else:                
                messages.append({"role":'system',
                            "content":system_message})                
                messages.append({
                            "role": "user",
                            "content": user_message[0]
                        })
                
            
        return messages
        

    def invoke(self,system_message:str,user_message:list,assistant_message:list,**kwargs):   

        """This method calls the API's of the various LLM providers. Supports single messages and conversation style messages.
        Params:
            
            system_message : Set the behaviour of the LLM by setting specific instructions here
            
            user_message : Pass the different questions in the conversation here in the form of an array in ascending order of time of question asked
            
            assistant_message : Pass the answers provided by the LLM for the previously asked questions in the form of an array in ascending order of time of the provided answer
            
            **kwargs : pass any other params to pass to the API call as arguments here"""              
        
        stream=kwargs.get('stream')
        if stream:
            raise Exception('invoke() method does not support streaming, use stream_invoke() instead')        

        messages = self.__message_generator(system_message,user_message,assistant_message)

        if self.__provider=='vertexai':
            
            # if int(time.time())-self.__iat>=3599:
            #     token,iat=self.__get_token_and_expiry_time()
            #     self.__token,self.__iat=token,iat
            
            data = {
                    "contents": messages,
                    "systemInstruction": {
                        "parts": [
                        {
                            "text": system_message
                        },
                    ]
                    },
                    "generationConfig": {
                        "maxOutputTokens": self.__max_tokens,
                        "temperature": self.__temperature,
                        "topP": self.__top_p,
                    }}
            
            url=self.__url.format(self.model)

            while self.__token_refreshal:
                # print('Waiting for token refresh')
                time.sleep(0.1)
            
            headers= {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.__token}"
            }

        elif self.__provider=='bedrock':   

            url=self.__url.format(self.__region,self.model.replace(':','%3A'))
            # print(url,'\n')

            data={'messages':messages,
                'system' : [{'text':system_message}],
                'inference_config' : {"maxTokens":self.__max_tokens,"temperature": self.__temperature,'topP':self.__top_p}}
            
            headers = self.__get_aws_headers(self.model,data)
        
        else:
            
            url=self.__url
            data = {
                    "messages": messages,
                    "model": self.model,
                    "temperature": self.__temperature,
                    "max_tokens": self.__max_tokens,
                    "top_p": self.__top_p,
                    }

            headers= {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.__token}"
            }

        if kwargs:
            for key,value in kwargs.items():
                data[key]=value
            
        response=post(url,headers=headers,data=json.dumps(data))
            
        return self.__chat_model_response(self.__provider,response)
        

    def stream_invoke(self,headers=None,**kwargs):
        raise Exception('This method is under development')