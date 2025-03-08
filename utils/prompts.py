GENERATE_SQL_PROMPT = """ Write the prompt to generate SQL query based on your use case here.
```<SCHEMA> {} </SCHEMA>```"""

SQL_CORRECTION_PROMPT = """
            You are a SQL syntax rectifier. Analyse the SQL and associated
            error with the provided schema and generate correct SQL with proper syntax.
            Very Important Note:
            Give only the SQL query as output without additional explanations, texts or strings. 

            ```<SCHEMA> {} </SCHEMA>```
        """