from typing import Protocol, Optional, Iterable


class DbConnection(Protocol):

    def test_connection(self) -> None:
        """Raises Connection error on connection failure"""

    def connect(self) -> None:
        """Connect to database"""

    def perform_query(self, query: str, bind_parameters: Optional[dict] = None) -> Iterable[dict]:
        """Performs query"""


class DbApiConnection:

    def __init__(self, connection):
        self._connection = connection
