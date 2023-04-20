# from app.db import *

from .utils import *

import os

def main():

    if database_integrity():
        print("database_integrity good")

if __name__ == '__main__':
    main()
