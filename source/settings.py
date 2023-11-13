# -*- coding: utf-8 -*-
import os
import sys
import time
import logging
import platform
import traceback

LOGO = "\n\
----------------------------------------------------------\n\
       __                _ _____               _          \n\
      / /  ___ _ __ ___ (_)__   \_ __ __ _  __| | ___     \n\
     / /  / _ \ '_ ` _ \| | / /\/ '__/ _` |/ _` |/ _ \    \n\
    / /__|  __/ | | | | | |/ /  | | | (_| | (_| |  __/    \n\
    \____/\___|_| |_| |_|_|\/   |_|  \__,_|\__,_|\___|    \n\
                                                          \n\
----------------------------------------------------------\n\
"
#=====================================================================
os.system("")
os.environ["NUMEXPR_MAX_THREADS"] = str(os.cpu_count())
SYSTEM_ROOT = os.path.abspath(os.path.dirname(__file__))
SYSTEM_FILE = os.path.join(SYSTEM_ROOT, "main.py")
CONFIGS_FILE = os.path.join(SYSTEM_ROOT, "configs.json")
LOG_DIR = os.path.join(SYSTEM_ROOT, "log")
ERROR_LOG_FILE = os.path.join(LOG_DIR, "error.log")
CONSOLE_LOG_FILE = os.path.join(LOG_DIR, "console.log")
SYSTEM_TIME = lambda : time.asctime(time.localtime(time.time()))
SYSTEM_TICK = lambda : time.time()
SYSTEM_TICK_MS = lambda : round(time.time() * 1000)
SYSTEM_PLATFORM = platform.system()
SYSTEM_PID = os.getpid()

os.makedirs(LOG_DIR, exist_ok=True)

class CriticalError(Exception):
    def __init__(self, message):
        self.message = message
   
    def __str__(self):
        return self.message

def RaiseErrorFile(path):
    if os.path.isfile(path):
        error = ""
        with open(path, "r") as file:
            error = file.read()
        if error == "":
            return
        raise CriticalError(error)

open(ERROR_LOG_FILE, 'w').close()
open(CONSOLE_LOG_FILE, 'w').close()
hr1 = logging.FileHandler(ERROR_LOG_FILE)
hr2 = logging.FileHandler(CONSOLE_LOG_FILE)
hr3 = logging.StreamHandler()
hr1.set_name("Critical Handler")
hr2.set_name("Console File Handler")
hr3.set_name("Console Stream Handler")
hr1.setLevel(logging.CRITICAL)
hr2.setLevel(logging.INFO)
hr3.setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers= [hr1, hr2, hr3])