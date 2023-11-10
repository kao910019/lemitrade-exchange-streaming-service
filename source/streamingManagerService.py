import json
import asyncio
import numpy as np

from libs.classes.consoleCommandClass import ConsoleCommand
from libs.classes.commandClass import Command

from libs.serviceBase import ServiceBase, RunService
from libs.configs import Configs
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
    async def StreamList(self):
        pass
    
    @ConsoleCommand(command=["stream"], subCommand=["subscribe", "sub"], message="stream subscribe [streams]: Subscribe streams")
    async def SubscribeStream(self):
        pass

    @ConsoleCommand(command=["stream"], subCommand=["unsubscribe", "usub"], message="stream unsubscribe [streams]: Unsubscribe streams")
    async def UnSubscribeStream(self):
        pass

    @ConsoleCommand(command=["service"], subCommand=["test"], message="service test : Service function test")
    async def CMD_Test(self):
        logging.info("Service Function Testing Start")
        # Kline Request
        await self.SendMessage(self.info["id"], "kline_request", {
            "exchange": "binance", "type": "spot", "symbols":["BTCUSDT", "ETHUSDT"], "periods":["15m", "15m"], "count":1000
        }, ack=True, timeout=60)
        logging.info("Service Function Testing Complete")

    # ========================================== Service Commands ==========================================
    # ------------------------------------------
    # Command : kline_request
    # Description : Receive request from robot manager service, make sure there're have all kline & listener on the redis
    # Message : {exchange, type, symbols, periods, count}
    # -------------------------------------------
    @Command("kline_request")
    async def KlineRequest(self, cls:str, clsId:str, message:dict = {}):
        keys = ["stream:info:{}:{}:{}:{}".format(message["exchange"], message["type"], s, p) for s, p in zip(message["symbols"], message["periods"])]
        print("keys", keys)
        # Get all streaming info on redis
        logging.info("KlineRequest: Get all streaming info on redis")
        result = await self.rs.r.mget(keys)
        print("result", result)
        # Check which is not streaming
        logging.info("KlineRequest: Check which is not streaming")
        indices = np.where(np.array(result) == None)[0]
        print("indices", indices)
        while len(indices) != 0:
            outputs = await self.GetServiceDict("StreamingListener")
            print("GetServiceDict", outputs)
            for id, info in outputs.items():
                symbols, periods = np.array(message["symbols"])[indices[:info["freeSubscribe"]]], np.array(message["periods"])[indices[:info["freeSubscribe"]]]
                msg = {"exchange":message["exchange"], "type":message["type"], "symbols":list(symbols), "periods":list(periods), "count":message["count"]}
                await self.SendMessage(id, "subscribe", msg, ack=True, timeout=60)
                indices = indices[info["freeSubscribe"]:]
                if len(indices) == 0:
                    break
        # Handler prepare kline
        logging.info("KlineRequest: Handler prepare kline")
        await self.SendMessage("StreamingHandler", "kline_prepare", message, ack=True, timeout=60)
        logging.info("KlineRequest: Assert kline already finish")
        # Assert kline already finish
        result = await self.rs.r.mget(keys)
        indices = np.where(np.array(result) == None, np.array(result) != None)[0]
        assert (len(indices) == 0)
        assert all([json.loads(data)["count"] >= message["count"] for data in result])

if __name__ == "__main__":
    asyncio.run(RunService(StreamingManagerService))