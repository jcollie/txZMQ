from twisted.python import log

from txZMQ import ZmqConnection
from zmq.core import constants
from zmq.core import error

class ZmqPushConnection(ZmqConnection):
    socketType = constants.PUSH

    def recv(self):
        raise TypeError('PUSH connections are send only')

class ZmqPullConnection(ZmqConnection):
    socketType = constants.PULL

    def send(self):
        raise TypeError('PULL connections are receive only')
