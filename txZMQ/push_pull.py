from twisted.python import log

from txZMQ import ZmqConnection
from zmq.core import constants
from zmq.core import error

class ZmqPushConnection(ZmqConnection):
    socketType = constants.PUSH

class ZmqPullConnection(ZmqConnection):
    socketType = constants.PULL
