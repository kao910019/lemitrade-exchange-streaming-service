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
        self.validPeriods = ["1m", "5m", "15m", "30m", "1h", "2h", "4h", "1d"] # short > long

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

    def KlineToData(self, kline:pd.DataFrame):
        data:dict = kline.to_dict("index")
        data = {str(t):json.dumps(d, default=str) for t, d in data.items()}
        return data

    def DataToKline(self, data:dict=None, timestamps:list=None, result:list=None):
        if data is None:
            data = {k: (json.loads(v) if v is not None else {"timestamp":k, "open":None, "high":None, "low":None, "close":None, "volume":0}) for k, v in zip(timestamps, result)}
        kline = pd.DataFrame.from_dict(data, "index")
        kline.index = pd.to_datetime(kline['timestamp'], unit='ms')
        kline["open"] = kline["open"].apply(np.float64)
        kline["high"] = kline["high"].apply(np.float64)
        kline["low"] = kline["low"].apply(np.float64)
        kline["close"] = kline["close"].apply(np.float64)
        kline["volume"] = kline["volume"].apply(np.float64)
        # kline["open"] = kline["open"].apply(Decimal)
        # kline["high"] = kline["high"].apply(Decimal)
        # kline["low"] = kline["low"].apply(Decimal)
        # kline["close"] = kline["close"].apply(Decimal)
        # kline["volume"] = kline["volume"].apply(Decimal)
        return kline
    
    # Load kline from exchange, sync to redis
    async def PrepareKline(self, tag:str, exchange:Exchange, symbol:str, period:str, count:int=1000):
        kline = await exchange.UpdateKline(symbol, period, count)
        kline.index = kline.index.astype(np.int64) // int(1e6)
        data = self.KlineToData(kline)
        await self.UpdateKline(tag, symbol, period, data)

    # Load kline from redis
    async def GetRedisKline(self, tag:str, symbol:str, period:str, count:int, since:int=None, to:int=None, missingProcess=True):
        timestamps = GenerateTimestamp(period, count, since, to)
        timestamps = timestamps.astype(str).tolist()
        if len(timestamps) == 0:
            return None
        result = await self.rs.r.hmget("kline:data:{}:{}:{}".format(tag, symbol, period), timestamps)
        if missingProcess:
            indices = np.where(np.array(result) == None)[0]
            if len(indices) != 0:
                missingTimestamps = (np.array(timestamps)[indices]).tolist()
                await self.SendMessage("StreamingHandler", "kline_missing", {"tag":tag, "symbol":symbol, "period":period, "timestamps":missingTimestamps}, ack=True, timeout=10)
        kline = self.DataToKline(timestamps=timestamps, result=result)
        return kline

    # Merge kline, merge 1 min kline to specific period
    async def MergeKlineWithPeriods(self, tag:str, symbol:str, periods:list, count:int, since:int=None, to:int=None):
        countList = [int((count*MillisecondPeriod(period))/MillisecondPeriod("1m")) for period in periods]
        kline = await self.GetRedisKline(tag, symbol, "1m", max(countList), since, to, missingProcess=False)
        if kline is None:
            return None
        outputs = {}
        for c, p in zip(countList, periods):
            try:
                k = kline.iloc[-c:].resample(p.replace("m", "min")).agg(
                    {'timestamp': 'first', 'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}
                    ).asfreq(p.replace("m", "min")).iloc[-1:]
                k.index = k.index.astype(np.int64) // int(1e6)
                outputs[p] = k
            except:
                pass
        return outputs
    
    async def UpdateKline(self, tag:str, symbol:str, period:str, data:dict):
        await self.rs.r.hset("kline:data:{}:{}:{}".format(tag, symbol, period), mapping=data)
        await self.SyncKline(tag, symbol, period)

    # ========================================== Console Commands ==========================================
    @ConsoleCommand(command=["service"], subCommand=["test"], message="service test : Service function test")
    async def CMD_Test(self):
        # timestamp = SYSTEM_TICK_MS()
        # exchange = self.GetExchange("binance", "spot")
        # await self.PrepareKline("binance:spot", exchange, "BTC/USDT", "1m", 1000)
        # kline = await self.GetRedisKline("binance:spot", "BTC/USDT", "15m", 1000)
        timestamp = SYSTEM_TICK_MS()
        to = FixTimestampToPeriod(timestamp, "1m", inputUnit="ms") # get longest timestamp
        since = FixTimestampToPeriod(timestamp, self.validPeriods[-1], inputUnit="ms") # get longest timestamp
        outputs:dict = await self.MergeKlineWithPeriods("binance:spot", "NEXOUSDT", self.validPeriods[1:], 1, since=since, to=to) # remove 1m
        print(outputs)
        
    
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
            if res["count"] >= message["count"] and timestamp:
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
    # ========================================== Event Commands ==========================================
    # ------------------------------------------
    # Command : kline_update
    # Description : Update kline data to redis server, it will calculate 1m kline to different period
    # Message : {exchangeTag, message}
    # -------------------------------------------
    @Command("kline_update")
    async def MSG_KlineUpdate(self, cls:str, clsId:str, message:dict = {}):
        exchangeTag, message = message["exchangeTag"], message["message"]
        symbol, period = message["symbol"], message["period"]
        assert period == "1m"
        timestamp = message["kline"]["timestamp"]
        await self.UpdateKline(exchangeTag, symbol, period, {str(timestamp): json.dumps(message["kline"])})
        since = FixTimestampToPeriod(timestamp, self.validPeriods[-1], inputUnit="ms") # get longest timestamp
        since = since - MillisecondPeriod(self.validPeriods[-1])
        outputs:dict = await self.MergeKlineWithPeriods(exchangeTag, symbol, self.validPeriods[1:], 2, since=since, to=timestamp) # remove 1m
        if outputs is None:
            return
        for p, kline in outputs.items():
            await self.UpdateKline(exchangeTag, symbol, p, self.KlineToData(kline))
        
if __name__ == "__main__":
    asyncio.run(RunService(StreamingHandlerService))