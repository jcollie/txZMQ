"""
ZeroMQ connection.
"""

from collections import deque, namedtuple

from zmq.core.socket import Socket
from zmq.core import constants, error

from zope.interface import implements
from twisted.internet import reactor
from twisted.internet.interfaces import IReadDescriptor
from twisted.internet.interfaces import IFileDescriptor
from twisted.internet import defer

from twisted.python import log


class ZmqEndpointType(object):
    """
    Endpoint could be "bound" or "connected".
    """
    Bind = "bind"
    Connect = "connect"


ZmqEndpoint = namedtuple('ZmqEndpoint', ['type', 'address'])

ZmqQueueEntry = namedtuple('ZmqQueueEntry', ['send_completed', 'messages'])

class ZmqConnection(object):
    """
    Connection through ZeroMQ, wraps up ZeroMQ socket.

    @cvar socketType: socket type, from ZeroMQ
    @cvar allowLoopbackMulticast: is loopback multicast allowed?
    @type allowLoopbackMulticast: C{boolean}
    @cvar multicastRate: maximum allowed multicast rate, kbps
    @type multicastRate: C{int}
    @cvar highWaterMark: hard limit on the maximum number of outstanding messages
        0MQ shall queue in memory for any single peer
    @type highWaterMark: C{int}

    @ivar factory: ZeroMQ Twisted factory reference
    @type factory: L{ZmqFactory}
    @ivar socket: ZeroMQ Socket
    @type socket: L{Socket}
    @ivar endpoints: ZeroMQ addresses for connect/bind
    @type endpoints: C{list} of L{ZmqEndpoint}
    @ivar fd: file descriptor of zmq mailbox
    @type fd: C{int}
    @ivar _write_queue: output message queue
    @type _write_queue: C{deque}
    """
    implements(IReadDescriptor, IFileDescriptor)

    socketType = None
    allowLoopbackMulticast = False
    multicastRate = 100
    highWaterMark = 0
    identity = None

    def __init__(self, factory, *endpoints):
        """
        Constructor.

        @param factory: ZeroMQ Twisted factory
        @type factory: L{ZmqFactory}
        @param endpoints: ZeroMQ addresses for connect/bind
        @type endpoints: C{list} of L{ZmqEndpoint}
        """
        self.factory = factory
        self.endpoints = endpoints
        self.socket = Socket(factory.context, self.socketType)

        self._waiting_recvs = deque()
        self._write_queue = deque()
        self._recv_parts = []
        self._disconnected = 0
        self._queued_read = None

        self._fd = self.socket.getsockopt(constants.FD)
        self.socket.setsockopt(constants.LINGER, self.factory.lingerPeriod)
        self.socket.setsockopt(constants.MCAST_LOOP, int(self.allowLoopbackMulticast))
        self.socket.setsockopt(constants.RATE, self.multicastRate)
        self.socket.setsockopt(constants.HWM, self.highWaterMark)
        if self.identity is not None:
            self.socket.setsockopt(constants.IDENTITY, self.identity)

        self._connectOrBind()

        self.factory.connections.add(self)

        self.factory.reactor.addReader(self)

    def shutdown(self):
        """
        Shutdown connection and socket.
        """
        if self.factory:
            self.factory.reactor.removeReader(self)

        if self._queued_read and self._queued_read.active():
            self._queued_read.cancel()

        if self.factory:
            self.factory.connections.discard(self)

        for waiting_recv in self._waiting_recvs:
            waiting_recv.errback(None)

        self.socket.close()
        self.socket = None

        self.factory = None
        self._disconnected = 1

    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__, self.factory, self.endpoints)

    def fileno(self):
        """
        Part of L{IFileDescriptor}.

        @return: The platform-specified representation of a file descriptor
                 number.
        """
        return self._fd

    def connectionLost(self, reason):
        """
        Called when the connection was lost.

        Part of L{IFileDescriptor}.

        This is called when the connection on a selectable object has
        been lost.  It will be called whether the connection was
        closed explicitly, an exception occurred in an event handler,
        or the other end of the connection closed it first.

        @param reason: A failure instance indicating the reason why the
                       connection was lost.  L{error.ConnectionLost} and
                       L{error.ConnectionDone} are of special note, but the
                       failure may be of other classes as well.
        """
        log.err(reason, "Connection to ZeroMQ lost in %r" % (self))
        if self.factory:
            self.factory.reactor.removeReader(self)
        if self._queued_read and self._queued_read.active():
            self._queued_read.cancel()

    def _readMultipart(self):
        """
        Read multipart in non-blocking manner, returns with ready
        message or raising exception (in case of no more messages
        available).
        """
        while True:
            self._recv_parts.append(self.socket.recv(constants.NOBLOCK))
            if not self.socket.rcvmore():
                result, self._recv_parts = self._recv_parts, []
                return result

    def doRead(self):
        """
        Some data is available for reading on your descriptor.

        ZeroMQ is signalling that we should process some events.

        Part of L{IReadDescriptor}.
        """

        if self._disconnected:
            return

        events = self.socket.getsockopt(constants.EVENTS)

        if self._waiting_recvs and (events & constants.POLLIN) == constants.POLLIN:
            while self._waiting_recvs:
                try:
                    message = self._readMultipart()
                    self._waiting_recvs.popleft().callback(message)

                except error.ZMQError as e:
                    if e.errno == constants.EAGAIN:
                        break

                    raise e

        if (events & constants.POLLOUT) == constants.POLLOUT:
            self._startWriting()

    def _startWriting(self):
        """
        Start delivering messages from the queue.
        """
        while self._write_queue:
            try:
                for option, message in self._write_queue[0].messages:
                    self.socket.send(message, constants.NOBLOCK | option)
                self._write_queue.popleft().send_completed.callback(None)
            except error.ZMQError as e:
                if e.errno == constants.EAGAIN:
                    break
                self._write_queue.popleft()
                raise e

    def logPrefix(self):
        """
        Part of L{ILoggingContext}.

        @return: Prefix used during log formatting to indicate context.
        @rtype: C{str}
        """
        return 'ZMQ'

    def send(self, message):
        """
        Send message via ZeroMQ.

        @param message: message data
        """
        send_completed = defer.Deferred()

        if not hasattr(message, '__iter__'):
            message = ZmqQueueEntry(send_completed, [(0, message)])

        else:
            message = ZmqQueueEntry(send_completed, [(constants.SNDMORE, part) for part in message])
            message.messages[-1] = (0, message.messages[-1][1])

        self._write_queue.append(message)
        self._startWriting()

        # we might have missed an event, queue a read
        if self._queued_read is None or self._queued_read.called:
            self._queued_read = reactor.callLater(0, self.doRead)
        
        return send_completed

    def recv(self):
        waiting_recv = defer.Deferred()
        self._waiting_recvs.append(waiting_recv)
        
        # queue a read
        if self._queued_read is None or self._queued_read.called:
            self._queued_read = reactor.callLater(0, self.doRead)
        
        return waiting_recv

    def _connectOrBind(self):
        """
        Connect and/or bind socket to endpoints.
        """
        for endpoint in self.endpoints:
            if endpoint.type == ZmqEndpointType.Connect:
                self.socket.connect(endpoint.address)
            elif endpoint.type == ZmqEndpointType.Bind:
                self.socket.bind(endpoint.address)
            else:
                assert False, "Unknown endpoint type %r" % endpoint
