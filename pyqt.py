#!/usr/bin/env python3

import datetime
import json
import sys

import PyQt5.QtWidgets as qtw
import PyQt5.QtCore as qtc
import PyQt5.QtWebSockets as qtws

class KorbitWsClient(qtc.QObject):
    received = qtc.pyqtSignal(dict)
    # closed = qtc.pyqtSignal()

    def __init__(self):
        super().__init__()
        
        self.websocket = qtws.QWebSocket()
        self.websocket.connected.connect(self.onConnected)
        # self.websocket.disconnected.connect(self.closed)
        # self.websocket.textMessageReceived.connect(self.onTextMessageReceived)
        self.websocket.textFrameReceived.connect(self.onTextMessageReceived)
        uri = "wss://ws.korbit.co.kr/v1/user/push"
        self.websocket.open(qtc.QUrl(uri))

    @qtc.pyqtSlot()
    def onConnected(self):
        now = datetime.datetime.now()
        timestamp = int(now.timestamp() * 1000)

        subscribe_fmt = {
            "accessToken": None,
            "timestamp": timestamp,
            "event": "korbit:subscribe",
            "data": {
                "channels": ["ticker:btc_krw"]
            }
        }
        subscribe_data = json.dumps(subscribe_fmt)
        self.websocket.sendTextMessage(subscribe_data)
        
    @qtc.pyqtSlot(str)
    def onTextMessageReceived(self, msg: str):
        data = json.loads(msg)
        self.received.emit(data)


class MainObject(qtc.QObject):
    def __init__(self):
        super().__init__()

        self.ws = KorbitWsClient()
        # self.producer_thread = qtc.QThread()
        # self.ws.moveToThread(self.producer_thread)
        self.ws.received.connect(self.print_data)
        # self.producer_thread.start()

    @qtc.pyqtSlot(dict)
    def print_data(self, data):
        timestamp = data.get('timestamp')
        data_dict = data.get('data')
        last = data_dict.get('last')
        now = datetime.datetime.fromtimestamp(int(timestamp) / 1000)
        

if __name__ == "__main__":
    import signal

    # signal.signal(signal.SIGINT, sigint_handler)
    app = qtc.QCoreApplication(sys.argv)
    signal.signal(signal.SIGINT, lambda *x: app.quit())
    main_object = MainObject()
    sys.exit(app.exec())
