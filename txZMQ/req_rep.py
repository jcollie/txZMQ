from twisted.python import log

from txZMQ import ZmqConnection
from zmq.core import constants
from zmq.core import error

class ZmqReqConnection(ZmqConnection):
    socketType = constants.REQ

    # def doRead(self):
    #     events = self.socket.getsockopt(constants.EVENTS)

    #     if (events & constants.POLLIN) == constants.POLLIN:
    #         try:
    #             message = self._readMultipart()

    #         except error.ZMQError as e:
    #             if e.errno != constants.EAGAIN:
    #                 raise e

    #         log.callWithLogger(self, self.messageReceived, message)

    #     if (events & constants.POLLOUT) == constants.POLLOUT:
    #         self._startWriting()

class ZmqRepConnection(ZmqConnection):
    socketType = constants.REP

    # def __init__(self, factory, *endpoints):
    #     self._waiting_to_send_reply = False
    #     ZmqConnection.__init__(self, factory, *endpoints)

    # def _startWriting(self):
    #     ZmqConnection._startWriting(self)

    #     if not self.queue:
    #         self._waiting_to_send_reply = False

    # def doRead(self):
    #     events = self.socket.getsockopt(constants.EVENTS)

    #     if not self._waiting_to_send_reply:
    #         if (events & constants.POLLIN) == constants.POLLIN:
    #             try:
    #                 message = self._readMultipart()
    #                 self._waiting_to_send_reply = True
    #             except error.ZMQError as e:
    #                 if e.errno != constants.EAGAIN:
    #                     raise e
                
    #             log.callWithLogger(self, self.messageReceived, message)

    #     if (events & constants.POLLOUT) == constants.POLLOUT:
    #         self._startWriting()
