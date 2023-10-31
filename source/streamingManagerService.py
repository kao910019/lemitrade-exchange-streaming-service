import asyncio
from typing import Dict
import multiprocessing as mp

from libs.classes.consoleCommandClass import ConsoleCommandClass, ConsoleCommand
from libs.classes.commandClass import CommandClass, Command

from libs.configs import Configs
from libs.serviceBase import ServiceBase
from libs.utils import *

from streamingListenerService import StreamingListenerService
from settings import *


class StreamingManagerService(ServiceBase):
    def __init__(self, configs:Configs):
        super().__init__(configs, "StreamingManager")

        self.console.AddModule(self.info["name"], StreamingManagerService, self)
        self.db.AddModule(self.info["id"], StreamingManagerService, self)

        self.listeners:Dict[str:dict] = {}
        
    async def Start(self):
        await super().Start()

    async def Close(self):
        await super().Close()

    async def SyncListenerLict(self):
        self.db.Find()

    # ========================================== Console Commands ==========================================
    @ConsoleCommand(command=["stream"], subCommand=["list"], message="stream list : List all listener streams")
    async def StreamList(self, command, subCommand, args):
        self.streams = {}
        for clsId in self.listeners.keys():
            streams = self.db.SendMessage(clsId, "get_streams", ack=True)
            print(" - {} : {}".format(clsId, streams))
            self.streams[clsId] = streams
    
    @ConsoleCommand(command=["stream"], subCommand=["subscribe", "sub"], message="stream subscribe [streams]: Subscribe streams")
    async def SubscribeStream(self, command, subCommand, args):
        pass

    @ConsoleCommand(command=["stream"], subCommand=["unsubscribe", "usub"], message="stream unsubscribe [streams]: Unsubscribe streams")
    async def UnSubscribeStream(self, command, subCommand, args):
        pass

    # ========================================== Service Commands ==========================================
    @Command("listener_ready")
    async def ListenerReady(self, cls:str, clsId:str, message:dict = {}):
        pass
        # SyncListenerDict

    # @Command("subscribe_streams")
    # async def SubscribeStreams(self, cls:str, clsId:str, message:dict = {}):
    #     await self.InitExchange(message["exchange"], message["type"])
    
    # @Command("unsubscribe_streams")
    # async def UnsubscribeStreams(self, cls:str, clsId:str, message:dict = {}):
    #     await self.InitExchange(message["exchange"], message["type"])