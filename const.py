import os

FILE_DIR = os.path.abspath(os.path.dirname(__file__))

if not os.getcwd() == FILE_DIR:
    os.chdir(FILE_DIR)

DATA_FILE = os.path.join(FILE_DIR, 'data.csv')
LAYOUT_FILE = os.path.join(FILE_DIR, 'app', 'layout.txt')
