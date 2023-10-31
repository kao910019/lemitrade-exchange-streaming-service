from typing import Dict
import multiprocessing as mp

from libs.classes.consoleCommandClass import ConsoleCommandClass, ConsoleCommand
from libs.classes.commandClass import CommandClass, Command

from libs.configs import Configs
from libs.serviceBase import ServiceBase
from libs.tradingViewDataFeed import TradingViewDataFeed
# from libs.exchange import Exchange
from libs.utils import *
from settings import *

class StreamingListenerService(ServiceBase):
    def __init__(self, configs:Configs):
        super().__init__(configs, "StreamingListener")

        self.console.AddModule(self.info["id"], StreamingListenerService, self)
        self.db.AddModule(self.info["id"], StreamingListenerService, self)

        self.tw = TradingViewDataFeed()
        # self.exchanges:Dict[str:Exchange] = {}
        
    async def Start(self):
        await super().Start()
        await self.db.SendMessage("StreamingManager", "listener_ready", self.info.to_dict())

    async def Close(self):
        await super().Close()

    async def GetExchange(self, exchange, type, create=True):
        tag = "{}_{}".format(exchange, type)
        # exchange = self.exchanges.get(tag)
        # # Init Exchange
        # if exchange is None and create:
        #     exchange = Exchange(exchange, type, console=self.console)
        #     await exchange.Start()
        #     self.exchanges[tag] = exchange
        # return exchange

    # ========================================== Service Commands ==========================================
    @Command("subscribe_streams")
    async def SubscribeStreams(self, cls:str, clsId:str, message:dict = {}):
        exchange = await self.GetExchange(message["exchange"], message["type"])
    
    @Command("unsubscribe_streams")
    async def UnsubscribeStreams(self, cls:str, clsId:str, message:dict = {}):
        exchange = await self.GetExchange(message["exchange"], message["type"])

    @Command("get_streams")
    async def GetStreams(self, cls:str, clsId:str, message:dict = {}):
        streams = {}
        for tag, exchange in self.exchanges.items():
            streams[tag] = exchange.streams
        return streams