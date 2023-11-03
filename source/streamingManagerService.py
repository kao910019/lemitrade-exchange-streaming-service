import json
import asyncio
import multiprocessing as mp
import numpy as np

from libs.classes.consoleCommandClass import ConsoleCommandClass, ConsoleCommand
from libs.classes.commandClass import CommandClass, Command

from libs.configs import Configs
from libs.serviceBase import ServiceBase
from libs.utils import *

from settings import *


class StreamingManagerService(ServiceBase):
    def __init__(self, configs:Configs):
        super().__init__(configs, "StreamingManager")

        self.console.AddModule(self.info["name"], StreamingManagerService, self)
        self.AddModule(self.info["name"], StreamingManagerService, self)

    async def Start(self):
        await super().Start()

    async def Close(self):
        await super().Close()
    # ========================================== Console Commands ==========================================
    @ConsoleCommand(command=["stream"], subCommand=["list"], message="stream list : List all listener streams")
    async def StreamList(self, command, subCommand, args):
        pass
    
    @ConsoleCommand(command=["stream"], subCommand=["subscribe", "sub"], message="stream subscribe [streams]: Subscribe streams")
    async def SubscribeStream(self, command, subCommand, args):
        pass

    @ConsoleCommand(command=["stream"], subCommand=["unsubscribe", "usub"], message="stream unsubscribe [streams]: Unsubscribe streams")
    async def UnSubscribeStream(self, command, subCommand, args):
        pass

    # ========================================== Service Commands ==========================================
    # ------------------------------------------
    # Command : kline_request
    # Description : Receive request from robot manager service, make sure there're have all kline & listener on the redis
    # Message : {exchange, type, symbols, periods, count}
    # -------------------------------------------
    @Command("kline_request")
    async def KlineRequest(self, cls:str, clsId:str, message:dict = {}):
        keys = ["stream:info:{}:{}:{}:{}".format(message["exchange"], message["type"], s, p) for s, p in zip(message["symbols"], message["periods"])]
        # Get all streaming info on redis
        result = await self.rs.r.mget(keys)
        # Check which is not streaming
        indices = np.where(np.array(result) == None, np.array(result) != None)[0]
        while len(indices) != 0:
            outputs = await self.GetServiceList("StreamingListener")
            for id, info in outputs.items():
                symbols, periods = np.array(message["symbols"])[indices[:info["freeSubscribe"]]], np.array(message["periods"])[indices[:info["freeSubscribe"]]]
                msg = {"exchange":message["exchange"], "type":message["type"], "symbols":symbols, "periods":periods, "count":message["count"]}
                await self.SendMessage(id, "subscribe", msg, ack=True, timeout=60)
                indices = indices[info["freeSubscribe"]:]
                if len(indices) == 0:
                    break
        # Handler prepare kline
        await self.SendMessage("StreamingHandler", "kline_prepare", message, ack=True, timeout=60)
        # Assert kline already finish
        result = await self.rs.r.mget(keys)
        indices = np.where(np.array(result) == None, np.array(result) != None)[0]
        assert (len(indices) == 0)
        assert all([json.loads(data)["count"] >= message["count"] for data in result])