import json
import asyncio
import numpy as np
from typing import Dict
from decimal import Decimal

from libs.classes.consoleCommandClass import ConsoleCommand
from libs.classes.commandClass import Command

from libs.serviceBase import ServiceBase, RunService
from libs.exchange import Exchange
from libs.configs import Configs
from libs.utils import *

from settings import *

class StreamingHandlerService(ServiceBase):
    def __init__(self, configs:Configs):
        super().__init__(configs, "StreamingHandler")
        self.validPeriods = ["15m", "1m"]

        self.console.AddModule(self.info["name"], StreamingHandlerService, self)
        self.AddModule(self.info["name"], StreamingHandlerService, self)

        self.exchanges:Dict[str:Exchange] = {}

    async def Start(self):
        await super().Start()

    async def Close(self):
        for exchange in self.exchanges.values():
            await exchange.Close()
        await super().Close()

    def GetExchange(self, exchangeName:str="binance", marketType:str="spot", tag=None) -> Exchange:
        if tag is None:
            tag = "{}:{}".format(exchangeName, marketType)
        # Init exchange
        if self.exchanges.get(tag) is None:
            exchangeName, marketType = tag.split(":")
            self.exchanges[tag] = Exchange(exchangeName, marketType, console=self.console)
        return self.exchanges[tag]

    async def SyncKline(self, tag, symbol, period):
        # Update current kline state to redis
        timestamp = SYSTEM_TICK_MS()
        keys = await self.rs.r.hkeys("kline:data:{}:{}:{}".format(tag, symbol, period))
        series = pd.Series(keys).sort_values(ignore_index = True)
        timestampBegin, timestampEnd = series.iloc[0], series.iloc[-1]
        data = {
            "symbol":symbol, "period":period, "updateTime": timestamp, "count": len(keys),
            "timestampBegin":timestampBegin, "timestampEnd":timestampEnd
        }
        await self.rs.r.set("kline:info:{}:{}:{}".format(tag, symbol, period), json.dumps(data))

    # Load kline from exchange, sync to redis
    async def PrepareKline(self, tag, exchange:Exchange, symbol:str, period:str, count=1000):
        kline = await exchange.UpdateKline(symbol, period, count)
        kline.index = kline.index.view(np.int64) // int(1e6)
        data = kline.to_dict("index")
        data = {str(t):json.dumps(d, default=str) for t, d in data.items()}
        await self.rs.r.hset("kline:data:{}:{}:{}".format(tag, symbol, period), mapping=data)
        await self.SyncKline(tag, symbol, period)

    # Load kline from redis
    async def GetRedisKline(self, tag, symbol, period, count, since=None, to=None):
        timestamps = GenerateTimestamp(period, count, since, to)
        timestamps = timestamps.astype(str).tolist()
        result = await self.rs.r.hmget("kline:data:{}:{}:{}".format(tag, symbol, period), timestamps)
        indices = np.where(np.array(result) == None)[0]
        if len(indices) != 0:
            missingTimestamps = (np.array(timestamps)[indices]).tolist()
            await self.SendMessage("StreamingHandler", "kline_missing", {"tag":tag, "symbol":symbol, "period":period, "timestamps":missingTimestamps}, ack=True, timeout=10)
        result = {k: json.loads(v) for k, v in zip(timestamps, result) if v is not None}
        kline = pd.DataFrame.from_dict(result, "index")
        kline.index = pd.to_datetime(kline['timestamp'], unit='ms')
        kline["open"] = kline["open"].apply(Decimal)
        kline["high"] = kline["high"].apply(Decimal)
        kline["low"] = kline["low"].apply(Decimal)
        kline["close"] = kline["close"].apply(Decimal)
        kline["volume"] = kline["volume"].apply(Decimal)
        return kline

    # ========================================== Console Commands ==========================================
    @ConsoleCommand(command=["service"], subCommand=["test"], message="service test : Service function test")
    async def CMD_Test(self):
        # timestamp = SYSTEM_TICK_MS()
        # exchange = self.GetExchange("binance", "spot")
        # await self.PrepareKline("binance:spot", exchange, "BTC/USDT", "1m", 1000)
        kline = await self.GetRedisKline("binance:spot", "BTC/USDT", "15m", 1000)
        print(kline, kline["open"]+kline["open"]/kline["close"])
    
    # ========================================== Service Commands ==========================================
    # ------------------------------------------
    # Command : kline_prepare
    # Description : Prepare target kline to redis
    # Message : {exchange, type, symbols, periods, count}
    # -------------------------------------------
    @Command("kline_prepare")
    async def MSG_KlinePrepare(self, cls:str, clsId:str, message:dict = {}):
        timestamp = SYSTEM_TICK_MS()
        tag = "{}:{}".format(message["exchange"], message["marketType"])
        exchange = self.GetExchange(message["exchange"], message["marketType"])
        keys = ["kline:info:{}:{}:{}".format(tag, s, p) for s, p in zip(message["symbols"], message["periods"])]
        result = await self.rs.r.mget(keys)
        indices = np.where(np.array(result) == None)[0]
        # Prepare kline witch is none
        for index in indices:
            await self.PrepareKline(tag, exchange, message["symbols"][index], message["periods"][index], message["count"])
        # Prepare kline witch count is not enough
        for res in result:
            if res is None:
                continue
            res = json.loads(res)
            if res["count"] >= message["count"] and timestamp :
                continue
            await self.PrepareKline(tag, exchange, res["symbol"], res["period"], message["count"])
    # ------------------------------------------
    # Command : kline_missing
    # Description : Sync and reply missing kline
    # Message : {exchangeTag, symbol, period, timestamps}
    # -------------------------------------------
    @Command("kline_missing")
    async def MSG_KlineMissing(self, cls:str, clsId:str, message:dict = {}):
        logging.info("KlineMissing: {}".format(message))

    # ------------------------------------------
    # Command : stream_handle
    # Description : Handle stream message from listener
    # Message : {exchangeTag, message}
    # -------------------------------------------
    @Command("stream_handle")
    async def MSG_StreamHandle(self, cls:str, clsId:str, message:dict = {}):
        exchange = self.GetExchange(tag=message["exchangeTag"])
        await asyncio.sleep(1)


        
if __name__ == "__main__":
    asyncio.run(RunService(StreamingHandlerService))