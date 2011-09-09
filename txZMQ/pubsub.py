"""
ZeroMQ PUB-SUB wrappers.
"""

from zmq.core import constants

from txZMQ.connection import ZmqConnection


class ZmqPubConnection(ZmqConnection):
    """
    Publishing in broadcast manner.
    """
    socketType = constants.PUB

class ZmqSubConnection(ZmqConnection):
    """
    Subscribing to messages.
    """
    socketType = constants.SUB

    def subscribe(self, tag):
        """
        Subscribe to messages with specified tag (prefix).

        @param tag: message tag
        @type tag: C{str}
        """
        self.socket.setsockopt(constants.SUBSCRIBE, tag)

    def unsubscribe(self, tag):
        """
        Unsubscribe from messages with specified tag (prefix).

        @param tag: message tag
        @type tag: C{str}
        """
        self.socket.setsockopt(constants.UNSUBSCRIBE, tag)
