import pytest
import os

@pytest.fixture(scope="session")
def moto_server():
    from moto.server import ThreadedMotoServer

    host = "127.0.0.1"
    port = 5543
    server = ThreadedMotoServer(ip_address=host, port=port)
    server.start()
    yield server
    server.stop()

