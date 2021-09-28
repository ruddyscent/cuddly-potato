#!/usr/bin/env python3

import datetime
import json
import sys

from pymongo import MongoClient

from PyQt5.QtCore import QObject, QCoreApplication, QUrl, QThread, pyqtSignal, pyqtSlot
from PyQt5.QtWebSockets import QWebSocket

class KorbitWsClient(QObject):
    received = pyqtSignal(dict)

    def __init__(self):
        super().__init__()
        
        self.websocket = QWebSocket()
        self.websocket.connected.connect(self.onConnected)
        self.websocket.textMessageReceived.connect(self.onTextMessageReceived)
        uri = 'wss://ws.korbit.co.kr/v1/user/push'
        self.websocket.open(QUrl(uri))

    @pyqtSlot()
    def onConnected(self):
        kst = datetime.timezone(datetime.timedelta(hours=9))
        now = datetime.datetime.now(kst)
        timestamp = int(now.timestamp() * 1000)

        subscribe_fmt = {
            'accessToken': None,
            'timestamp': timestamp,
            'event': 'korbit:subscribe',
            'data': {
                'channels': [
                    'ticker',
                    'orderbook',
                    'transaction',
                ]
            }
        }
        subscribe_data = json.dumps(subscribe_fmt)
        self.websocket.sendTextMessage(subscribe_data)
        
    @pyqtSlot(str)
    def onTextMessageReceived(self, msg: str):
        data = json.loads(msg)
        self.received.emit(data)


class MainObject(QObject):
    def __init__(self):
        super().__init__()

        self.ws = KorbitWsClient()
        self.producer_thread = QThread()
        self.ws.moveToThread(self.producer_thread)
        self.ws.received.connect(self.print_data)
        self.producer_thread.start()

        client = MongoClient()
        self.db = client.korbit
        
        cols = self.db.list_collection_names()
        for col in ['ticker', 'transaction', 'orderbook']:
            if col in cols:
                continue

            self.db.create_collection(
                col, 
                timeseries = {
                    'timeField': 'timestamp',
                    'metaField': 'currency_pair',
                    'granularity': 'seconds'
                    }
                )

    @pyqtSlot(dict)
    def print_data(self, data):
        data_dict = data['data']
        event = data['event']
        
        if event in ['korbit:push-ticker', 'korbit:push-transaction', 'korbit:push-orderbook']:
            del data_dict['channel']
            
            timestamp = int(data_dict['timestamp'])
            kst = datetime.timezone(datetime.timedelta(hours=9))
            data_dict['timestamp'] = datetime.datetime.fromtimestamp(int(timestamp) / 1000, kst)
            
        if event == 'korbit:push-ticker':
            numeric_field = ['ask', 'bid', 'change', 'high', 'last', 'low', 'open', 'volume']
            for field in numeric_field:
                data_dict[field] = float(data_dict[field])
            self.db.ticker.insert_one(data_dict)

        elif event == 'korbit:push-transaction':
            numeric_field = ['amount', 'price']
            for field in numeric_field:
                data_dict[field] = float(data_dict[field])
            self.db.transaction.insert_one(data_dict)

        elif event == 'korbit:push-orderbook':
            for ask in data_dict['asks']:
                ask['amount'] = float(ask['amount'])
                ask['price'] = float(ask['price'])
                
            for bid in data_dict['bids']:
                bid['amount'] = float(bid['amount'])
                bid['price'] = float(bid['price'])

            self.db.orderbook.insert_one(data_dict)

        else:
            pass

    
if __name__ == '__main__':
    import signal

    app = QCoreApplication(sys.argv)
    signal.signal(signal.SIGINT, lambda *x: app.quit())
    main_object = MainObject()
    sys.exit(app.exec())
