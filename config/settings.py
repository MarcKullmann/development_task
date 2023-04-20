"""
FOR DEVELOPMENT ONLY, NEVER STORE YOUR CREDENTIALS INTO THE ENVIRONMENT 
VARIABLES!!!

The settings.py file contains the configuration settings for the database
connectios.
"""

import os

try:
    if os.environ['ISDOCKER'] == 'true':
        DATABASE = {
            'host': "db",
            'port': "5432",
            'user': "dev",
            'password': "devp4ssword",
            'name': "lzdb",
        }
    print(DATABASE)
except KeyError:
    DATABASE = {
        'host': "127.0.0.1",
        'port': "5432",
        'user': "dev",
        'password': "devp4ssword",
        'name': "lzdb",
    }
