from typing import Dict
import multiprocessing as mp

from libs.classes.consoleCommandClass import ConsoleCommandClass, ConsoleCommand
from libs.classes.commandClass import CommandClass, Command

from libs.serviceBase import ServiceBase
# from libs.exchange import Exchange
from libs.utils import *
from libs.configs import Configs

from streamingHandlerService import StreamingHandlerService
from settings import *

class StreamingHandlerService(ServiceBase):
    def __init__(self):
        super().__init__("StreamingHandler")

        self.console.AddModule(self.info["id"], StreamingHandlerService, self)
        self.db.AddModule(self.info["id"], StreamingHandlerService, self)

    async def Start(self):
        await super().Start()

    async def Close(self):
        await super().Close()
