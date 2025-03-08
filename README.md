# Chat-with-Database

## Overview

A Flask API to chat with your Database that uses API's provided by vertexAI, groq, bedrock or openai LLM providers under the hood

## Higlights 

- Includes a chat_models class that handles the complexities of sending requests to the LLM endpoints.
- Includes a connection pooling class to handle DB connections
- Utilises gevent eventlets for non blocking API calls to the LLMs