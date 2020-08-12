"""Exceptions raised by the protocol."""


class InvalidActionError(Exception):
    """Raised when an invalid action is attempted."""

    pass


class ServerError(Exception):
    """Raised when there is an issue related to the listening server."""

    pass


class ConnectError(Exception):
    """Generic connection failure."""

    pass


class TimeoutConnectError(ConnectError):
    """Raised when a connection fails due to a timeout."""

    pass


class DataConnectError(ConnectError):
    """Raised when data is unable to be read from a connection."""

    pass


class ResponseError(ConnectError):
    """Raised when a connection returns an invalid response."""

    pass


class DisconnectedError(Exception):
    """Raised when the node has become isolated from the network."""

    pass
