import os

FILE_DIR = os.path.abspath(os.path.dirname(__file__))

if not os.getcwd() == FILE_DIR:
    os.chdir(FILE_DIR)

DATA_FILE = os.path.join(FILE_DIR, 'data.csv')
LAYOUT_FILE = os.path.join(FILE_DIR, 'app', 'layout.txt')

_LOG = False


def enable_logging(enable=True):
    """
    Enable logging in the application
    :param: enable: the boolean to enable
    :return: none
    """
    global _LOG
    _LOG = enable


def log(msg):
    """
    If logging is enabled, log the message to stdout
    :param msg: the message to log
    :return: None
    """
    if _LOG:
        print(msg)
