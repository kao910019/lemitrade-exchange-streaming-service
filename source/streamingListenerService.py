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

        self.exchanges:Dict[str:Exchange] = {}
        self.listeners = {}

        self.info["totalSubscribe"] = 0
        self.info["freeSubscribe"] = 10*int(80.0 - self.process.cpu_percent())

    async def Start(self):
        await super().Start()

    async def Close(self):
        await super().Close()

    async def GetExchange(self, exchangeName, type):
        tag = "{}:{}".format(exchangeName, type)
        exchange:Exchange = self.exchanges.get(tag)
        # Init Exchange
        if exchange is None:
            self.exchanges[tag] = Exchange(exchangeName, type, console=self.console)
            exchange:Exchange = self.exchanges.get(tag)
        return exchange

    async def HealthCheck(self):
        await super().HealthCheck()
        if self.info["cpu"] > 90.0:
            # Release Subscribe to other listener
            self.info["freeSubscribe"] = 0
        elif self.info["cpu"] < 80.0:
            self.info["freeSubscribe"] = 10*int(80.0 - self.info["cpu"])
        self.info["totalSubscribe"] = sum([len(streams) for streams in self.GetStreams().values()])

    def GetStreams(self):
        streams = {}
        for listener in self.listeners.values():
            if streams.get(listener["tag"]) is None:
                streams[listener["tag"]] = []
            streams[listener["tag"]].extend(listener["streams"])
        return streams

    async def CreateListener(self, exchange:Exchange, stream:str):
        tag = "{}:{}".format(exchange.info["name"], exchange.info["marketType"])
        id = SYSTEM_TICK_MS()
        target = "Listener:{}:Open".format(id)
        await self.CreateSemaphore(target)
        listener = {
            "id": id, "task":"Listener:{}".format(id), "tag": tag, 
            "websocket": self.StartWebsocket(id, exchange.GetStreamingURL(), stream),
            "maxSubscribeNumber": exchange.settings["maxSubscribeNumber"], "command":"SUBSCRIBE", "streams": []
        }
        self.taskManager.CreateTask(listener["task"], listener["websocket"].ClientLoopTask)
        # Wait Listener Ready
        await self.SemaphoreTake(target, 10)
        return listener

    async def CloseListener(self, listener):
        await listener["websocket"].Close()
        self.taskManager.CloseTask(listener["task"])

    async def SubscribeStreams(self, exchange:Exchange, streams:list):
        while len(streams) > 0:
            for listener in self.listeners.values():
                # Delete streams which alreay streaming
                if len(listener["streams"]) != 0 and listener["websocket"].started:
                    streams = np.delete(np.array(streams, dtype=object), np.where(
                        np.array(streams, dtype=object)[:, None] == \
                        np.array(listener["streams"], dtype=object)[None, :])[0])
                if len(streams) == 0:
                    return
            for listener in self.listeners.values():
                # Subscribe new streams
                if len(listener["streams"]) < listener["maxSubscribeNumber"] and listener["websocket"].started:
                    amount = min(len(streams), listener["maxSubscribeNumber"] - len(listener["streams"]))
                    await self.ListenerSubscribeStreams(listener, streams)
                    streams = streams[amount:]
                if len(streams) == 0:
                    return
            # Create New Listener
            listener = await self.CreateListener(exchange, streams[0])
            self.listeners[listener["id"]] = listener

    async def UnsubscribeStreams(self, streams):
        removeList = []
        for listener in self.listeners.values():
            sameStreams = np.array([])
            if len(listener["streams"]) != 0 and listener["websocket"].started:
                sameStreams = np.take(np.array(streams, dtype=object), np.where(
                    np.array(streams, dtype=object)[:, None] ==  \
                    np.array(listener["streams"], dtype=object)[None, :])[0])
            if len(sameStreams) and listener["websocket"].started:
                await self.ListenerUnsubscribeStreams(listener, sameStreams.tolist())
                # Delete Empty Listener
                if len(listener["streams"]) == 0:       
                    await self.CloseListener(listener)
                    removeList.append(listener.id)
        for id in removeList:
            self.listeners.pop(id)

    def StartWebsocket(self, id, url, stream:str) -> WebsocketClass:
        websocket = WebsocketClass(
            "client", url if stream is None else "{}{}".format(url, stream), id=id, title="Exchange Listener", 
            on_message = self.WebsocketOnMessage, 
            on_close = self.WebsocketOnClose, 
            on_error= self.WebsocketOnError, 
            on_open = self.WebsocketOnOpen
        )
        return websocket

    async def WebsocketOnOpen(self, id, client):
        logging.info("Exchange Listener {} Websocket Open".format(id))
        await self.SemaphoreGive("Listener:{}:Open".format(id))

    async def WebsocketOnMessage(self, id, client, message):
        message:dict = json.loads(message)
        # print("WebsocketOnMessage", message)
        stream = message.get("stream")
        if stream is not None:
            pass
            # self.SendMessage("StreamingHandler", "stream", message={"tag":self.listeners[id]["tag"], "message":message})
            # self.EnQueue(self.messageQueue, message)
        else:
            await self.ListenerStreamsCheck(id, message)

    async def WebsocketOnError(self, id, client, error):
        logging.error("Exchange Listener {} Websocket Error: {}".format(id, error))

    async def WebsocketOnClose(self, id):
        logging.info("Exchange Listener {} Websocket Close".format(id))

    async def ListenerStreamsCheck(self, id, message):
        listener = self.listeners[id]
        if listener["command"] in ["SUBSCRIBE", "UNSUBSCRIBE"] and message.get("result", "") is None:
            listener["command"] = "LIST_SUBSCRIPTIONS"
            await listener["websocket"].Send(json.dumps({"method": "LIST_SUBSCRIPTIONS", "id": id}))
        elif listener["command"] == "LIST_SUBSCRIPTIONS" and type(message.get("result")) == type([]):
            listener["streams"] = message["result"]
            listener["command"] = None
            await self.SemaphoreGive("Listener:{}:SUBSCRIPTIONS".format(listener["websocket"].id))
    
    async def ListenerSubscribeStreams(self, listener, streams):
        if not listener["websocket"].started:
            raise ValueError("# Websocket is not connected")
        listener["command"] = "SUBSCRIBE"
        while listener["command"] is not None:
            if listener["command"] == "SUBSCRIBE":
                await self.CreateSemaphore("Listener:{}:SUBSCRIPTIONS".format(listener["websocket"].id))
                await listener["websocket"].Send(json.dumps({"method": "SUBSCRIBE", "params": streams, "id": listener["websocket"].id}))
            await self.SemaphoreTake("Listener:{}:SUBSCRIPTIONS".format(listener["websocket"].id), 2)

    async def ListenerUnsubscribeStreams(self, listener, streams):
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
        await self.SendMessage(self.info["id"], "subscribe", {
            "exchange": "binance", "type": "spot", "symbols":["BTCUSDT", "ETHUSDT"], "periods":["15m", "15m"], "count":1000
        }, ack=True, timeout=60)
        logging.info("Service Function Testing Complete")
    
    @ConsoleCommand(command=["listener"], subCommand=["list", "l"], message="listener list : List all listener in service")
    async def CMD_ListenerList(self):
        for id, listener in self.listeners.items():
            logging.info(" Listener - {}: \n - - command: {}\n - - streams: {}\n".format(id, listener["command"], listener["streams"]))
    # ========================================== Service Commands ==========================================
    # ------------------------------------------
    # Command : subscribe
    # Description : Subscribe Stream
    # Message : {exchange, type, symbols, periods}
    # -------------------------------------------
    @Command("subscribe")
    async def Subscribe(self, cls:str, clsId:str, message:dict = {}):
        tag = "{}:{}".format(message["exchange"], message["type"])
        exchange = await self.GetExchange(message["exchange"], message["type"])
        # Get All Streams
        streams = [exchange.GetSymbolStream(s, p) for s, p in zip(message["symbols"], message["periods"])]
        # Search/Apply Free Listener
        await self.SubscribeStreams(exchange, streams)
        
    # ------------------------------------------
    # Command : unsubscribe
    # Description : Unsubscribe Stream
    # Message : {exchange, type, symbols, periods}
    # -------------------------------------------
    @Command("unsubscribe")
    async def Unsubscribe(self, cls:str, clsId:str, message:dict = {}):
        tag = "{}:{}".format(message["exchange"], message["type"])
        exchange = await self.GetExchange(message["exchange"], message["type"])
        # Get All Streams
        streams = [exchange.GetSymbolStream(s, p) for s, p in zip(message["symbols"], message["periods"])]
        # Search/Apply Free Listener
        await self.UnsubscribeStreams(exchange, streams)

if __name__ == "__main__":
    asyncio.run(RunService(StreamingListenerService))