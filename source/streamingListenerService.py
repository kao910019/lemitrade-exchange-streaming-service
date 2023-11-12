import json
import logging
import asyncio
from typing import Dict

from libs.classes.consoleCommandClass import ConsoleCommand
from libs.classes.commandClass import Command
from libs.classes.asyncWebsocketClass import WebsocketClass

from libs.serviceBase import ServiceBase, RunService
from libs.configs import Configs
from libs.exchange import Exchange
from libs.utils import *
from settings import *

class StreamingListenerService(ServiceBase):
    def __init__(self, configs:Configs):
        super().__init__(configs, "StreamingListener")
        
        self.console.AddModule(self.info["name"], StreamingListenerService, self)
        self.AddModule(self.info["name"], StreamingListenerService, self)

        self.exchangesInfo:Dict[str:Exchange] = {}
        self.listeners = {}

        self.info["totalSubscribe"] = 0
        self.info["freeSubscribe"] = 10*int(80.0 - self.process.cpu_percent())

    async def Start(self):
        await super().Start()

    async def Close(self):
        # Unsubscribe streams
        for id in list(self.listeners.keys()):
            exchange = self.exchangesInfo[self.listeners[id]["tag"]]["exchange"]
            await self.UnsubscribeStreams(exchange, self.listeners[id]["streams"])
        # Delete streaming info
        for tag, exchangeInfo in self.exchangesInfo.items():
            await self.DeleteStreamInfo("symbols", tag, exchangeInfo["symbols"])
            await self.DeleteStreamInfo("clients", tag, exchangeInfo["clients"])
        for tag, exchangeInfo in self.exchangesInfo.items():
            await exchangeInfo["exchange"].Close()
        
        await super().Close()

    async def GetExchange(self, exchangeName:str, marketType:str) -> Exchange:
        tag = "{}:{}".format(exchangeName, marketType)
        # Init exchange
        if self.exchangesInfo.get(tag) is None:
            self.exchangesInfo[tag] = {"exchange": Exchange(exchangeName, marketType, console=self.console), "symbols":[], "clients":[]}
        return self.exchangesInfo[tag]["exchange"]

    async def HealthCheck(self):
        await super().HealthCheck()
        if self.info["cpu"] > 90.0:
            # Release subscribe to other listener
            self.info["freeSubscribe"] = 0
        elif self.info["cpu"] < 80.0:
            self.info["freeSubscribe"] = 10*int(80.0 - self.info["cpu"])
        # Check if any listener is overdate (over 12h)
        for id in self.listeners.keys():
            if ((self.info["updateTime"] - id) > MillisecondPeriod("12h")) and self.listeners[id]["state"] != "overdate":
                self.listeners[id]["state"] = "overdate"
                self.taskManager.CreateTask("Listener {} Overdate Process".format(id), self.ListenerOverdateTransfer, id)

    async def ListenerOverdateTransfer(self, id):
        logging.info("Listener {} overdate, transfer to new listener...".format(id))
        listener = self.listeners[id]
        exchange = self.exchangesInfo[listener["tag"]]["exchange"]
        # Search/Apply free listener
        await self.SubscribeStreams(exchange, listener["streams"])
        # Remove overdate listener
        await self.ListenerUnsubscribeStreams(listener, listener["streams"])
        await self.CloseListener(listener)
        self.listeners.pop(id)
        logging.info("Listener {} overdate transfer complete".format(id))

    def GetStreams(self):
        streams = {}
        for listener in self.listeners.values():
            if streams.get(listener["tag"]) is None:
                streams[listener["tag"]] = []
            streams[listener["tag"]].extend(listener["streams"])
        return streams

    async def UpdateStreamInfo(self, StreamType:str, tag:str, symbols:list):
        data = {"updateTime": SYSTEM_TICK_MS(), "serviceID":self.info["id"]}
        pairs = {symbol:json.dumps(data) for symbol in symbols}
        await self.rs.r.hset("stream:{}:{}".format(StreamType, tag), mapping=pairs)

    async def DeleteStreamInfo(self, StreamType:str, tag:str, symbols:list):
        for symbol in symbols:
            self.rs.pipe.hdel("stream:{}:{}".format(StreamType, tag), symbol)
        await self.rs.pipe.execute()

    async def CreateListener(self, exchange:Exchange, stream:str):
        tag = "{}:{}".format(exchange.info["name"], exchange.info["marketType"])
        id = SYSTEM_TICK_MS()
        url = exchange.GetStreamingURL()
        target = "Listener:{}:Open".format(id)
        await self.CreateSemaphore(target)
        listener = {
            "id": id, "task":"Listener:{}".format(id), "tag": tag, "state": "starting",
            "websocket": self.CreateWebsocket(id, tag, url, stream),
            "maxSubscribeNumber": exchange.settings["maxSubscribeNumber"], "command":"SUBSCRIBE", "streams": []
        }
        self.taskManager.CreateTask(listener["task"], listener["websocket"].ClientLoopTask)
        # Wait listener ready
        await self.SemaphoreTake(target, 10)
        listener["state"] = "running"
        return listener

    async def CloseListener(self, listener):
        await listener["websocket"].Close()
        self.taskManager.CloseTask(listener["task"])

    async def SubscribeStreams(self, exchange:Exchange, streams:list):
        tag = "{}:{}".format(exchange.info["name"], exchange.info["marketType"])
        while len(streams) > 0:
            for listener in self.listeners.values():
                if (listener["tag"] != tag) or (listener["state"] == "overdate"):
                    continue
                # Delete streams which alreay streaming
                if len(listener["streams"]) != 0 and listener["websocket"].started:
                    streams = np.delete(np.array(streams, dtype=object), np.where(
                        np.array(streams, dtype=object)[:, None] == \
                        np.array(listener["streams"], dtype=object)[None, :])[0]).tolist()
                if len(streams) == 0:
                    return
            for listener in self.listeners.values():
                if (listener["tag"] != tag) or (listener["state"] == "overdate"):
                    continue
                # Subscribe new streams
                if len(listener["streams"]) < listener["maxSubscribeNumber"] and listener["websocket"].started:
                    amount = min(len(streams), listener["maxSubscribeNumber"] - len(listener["streams"]))
                    await self.ListenerSubscribeStreams(listener, streams[:amount])
                    streams = streams[amount:]
                if len(streams) == 0:
                    return
            # Create new listener
            listener = await self.CreateListener(exchange, streams[0])
            self.listeners[listener["id"]] = listener

    async def UnsubscribeStreams(self, exchange:Exchange, streams):
        tag = "{}:{}".format(exchange.info["name"], exchange.info["marketType"])
        removeList = []
        for listener in self.listeners.values():
            sameStreams = np.array([])
            if (listener["tag"] != tag) or (listener["state"] == "overdate"):
                continue
            if len(listener["streams"]) != 0 and listener["websocket"].started:
                sameStreams = np.take(np.array(streams, dtype=object), np.where(
                    np.array(streams, dtype=object)[:, None] ==  \
                    np.array(listener["streams"], dtype=object)[None, :])[0])
            if len(sameStreams) and listener["websocket"].started:
                await self.ListenerUnsubscribeStreams(listener, sameStreams.tolist())
                # Delete empty listener
                if len(listener["streams"]) == 0:
                    await self.CloseListener(listener)
                    removeList.append(listener["id"])
        for id in removeList:
            self.listeners.pop(id)

    def CreateWebsocket(self, id, tag, url, stream:str) -> WebsocketClass:
        websocket = WebsocketClass(
            "client", url if stream is None else "{}{}".format(url, stream), id=id, title="Exchange Listener", 
            on_message = lambda id, client, message: self.WebsocketOnMessage(id, tag, client, message), 
            on_close = self.WebsocketOnClose, 
            on_error= self.WebsocketOnError, 
            on_open = self.WebsocketOnOpen
        )
        return websocket

    async def WebsocketOnOpen(self, id, client):
        logging.info("Exchange Listener {} Websocket Open".format(id))
        await self.SemaphoreGive("Listener:{}:Open".format(id))

    async def WebsocketOnMessage(self, id, tag, client, message):
        message:dict = json.loads(message)
        stream = message.get("stream")
        if stream is not None:
            await self.SendMessage("StreamingHandler", "stream_handle", message={"tag":tag, "message":message})
        else:
            await self.ListenerStreamsCheck(id, message)

    async def WebsocketOnError(self, id, client, error):
        logging.error("Exchange Listener {} Websocket Error: {}".format(id, error))
        self.listeners[id]["state"] = "error"

    async def WebsocketOnClose(self, id):
        logging.info("Exchange Listener {} Websocket Close".format(id))
        self.listeners[id]["state"] = "closing"

    async def ListenerStreamsCheck(self, id, message:dict):
        listener = self.listeners[id]
        if listener["command"] in ["SUBSCRIBE", "UNSUBSCRIBE"] and message.get("result", "") is None:
            listener["command"] = "LIST_SUBSCRIPTIONS"
            await listener["websocket"].Send(json.dumps({"method": "LIST_SUBSCRIPTIONS", "id": id}))
        elif listener["command"] == "LIST_SUBSCRIPTIONS" and type(message.get("result")) == type([]):
            listener["streams"] = message["result"]
            listener["command"] = None
            await self.SemaphoreGive("Listener:{}:SUBSCRIPTIONS".format(listener["websocket"].id))
    
    async def ListenerSubscribeStreams(self, listener, streams:list):
        if not listener["websocket"].started:
            raise ValueError("# Websocket is not connected")
        listener["command"] = "SUBSCRIBE"
        while listener["command"] is not None:
            if listener["command"] == "SUBSCRIBE":
                await self.CreateSemaphore("Listener:{}:SUBSCRIPTIONS".format(listener["websocket"].id))
                await listener["websocket"].Send(json.dumps({"method": "SUBSCRIBE", "params": streams, "id": listener["websocket"].id}))
            await self.SemaphoreTake("Listener:{}:SUBSCRIPTIONS".format(listener["websocket"].id), 2)

    async def ListenerUnsubscribeStreams(self, listener, streams:list):
        if not listener["websocket"].started:
            raise ValueError("# Websocket is not connected")
        listener["command"] = "UNSUBSCRIBE"
        while listener["command"] is not None:
            if listener["command"] == "UNSUBSCRIBE":
                await self.CreateSemaphore("Listener:{}:SUBSCRIPTIONS".format(listener["websocket"].id))
                await listener["websocket"].Send(json.dumps({"method": "UNSUBSCRIBE", "params": streams, "id": listener["websocket"].id}))
            await self.SemaphoreTake("Listener:{}:SUBSCRIPTIONS".format(listener["websocket"].id), 2)
    # ========================================== Console Commands ==========================================
    @ConsoleCommand(command=["service"], subCommand=["test"], message="service test : Service function test")
    async def CMD_Test(self):
        logging.info("Service Function Testing Start")
        # Kline Request
        exchange = await self.GetExchange("binance", "spot")
        await exchange.Start()
        targetSymbols = [symbol for symbol in list(exchange.symbolsDict.values()) if "USDT" in symbol]
        targetSymbols = [symbol for symbol in targetSymbols if "_" not in symbol]
        targetSymbols = [symbol for symbol in targetSymbols if ":" not in symbol]
        logging.info("Try to Subscribe {} Symbols".format(len(targetSymbols)))
        
        await self.SendMessage(self.info["id"], "subscribe", {
            "exchange": "binance", "marketType": "spot", "streamType":"symbols", "symbols":targetSymbols
        }, ack=True, timeout=60)
        logging.info("Service Function Testing Complete")
    
    @ConsoleCommand(command=["listener"], subCommand=["list", "l"], message="listener list : List all listener in service")
    async def CMD_ListenerList(self):
        for id, listener in self.listeners.items():
            logging.info(" Listener - {}: \n - - command: {}\n - - streams: {}\n".format(id, listener["command"], listener["streams"]))
    # ========================================== Service Commands ==========================================
    # ------------------------------------------
    # Command : subscribe
    # Description : 
    #  - Subscribe Stream, (1 min period only)
    #  - - Stream Type within "clients" & "symbols"
    # Message : {exchange, marketType, streamType, keys}
    # -------------------------------------------
    @Command("subscribe")
    async def MSG_Subscribe(self, cls:str, clsId:str, message:dict = {}):
        tag = "{}:{}".format(message["exchange"], message["marketType"])
        exchange = await self.GetExchange(message["exchange"], message["marketType"])
        streamType = message["streamType"]
        # Get all streams
        if streamType == "symbols":
            streams = [exchange.GetSymbolStream(s) for s in message[streamType]]
        elif streamType == "clients":
            streams = message[streamType]
        # Search/Apply free listener
        await self.SubscribeStreams(exchange, streams)
        self.exchangesInfo[tag][streamType].extend(message[streamType])
        self.exchangesInfo[tag][streamType] = list(set(self.exchangesInfo[tag][streamType]))
        # Update streaming info on redis
        await self.UpdateStreamInfo(streamType, tag, message[streamType])
        # Update service info
        self.info["totalSubscribe"] = sum([len(streams) for streams in self.GetStreams().values()])
        
    # ------------------------------------------
    # Command : unsubscribe
    # Description : 
    #  - Unsubscribe Stream, (1 min period only)
    #  - - Stream Type within "clients" & "symbols"
    # Message : {exchange, marketType, streamType, symbols/clients}
    # -------------------------------------------
    @Command("unsubscribe")
    async def MSG_Unsubscribe(self, cls:str, clsId:str, message:dict = {}):
        tag = "{}:{}".format(message["exchange"], message["marketType"])
        exchange = await self.GetExchange(message["exchange"], message["marketType"])
        streamType = message["streamType"]
        # Get all streams
        if streamType == "symbols":
            streams = [exchange.GetSymbolStream(s) for s in message[streamType]]
        elif streamType == "clients":
            streams = message[streamType]
        # Search/Apply free listener
        await self.UnsubscribeStreams(exchange, streams)
        for symbol in message["keys"]:
            self.exchangesInfo[tag][streamType].remove(symbol)
        # Delete streaming info on redis
        await self.DeleteStreamInfo(streamType, tag, message[streamType])
        # Update service info
        self.info["totalSubscribe"] = sum([len(streams) for streams in self.GetStreams().values()])

if __name__ == "__main__":
    asyncio.run(RunService(StreamingListenerService))