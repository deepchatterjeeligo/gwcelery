import unittest

from ligo.gracedb import rest
import pytest
from pytest_socket import disable_socket

from .. import app


@pytest.fixture(scope='session', autouse=True)
def celeryconf():
    new_conf = dict(
        broker_url='memory://',
        result_backend='cache+memory://',
        gcn_bind_address='127.0.0.1',
        gcn_bind_port=53410,
        gcn_remote_address='127.0.0.1',
        task_always_eager=True,
        task_eager_propagates=True,
        lvalert_host='lvalert.invalid',
        gracedb_host='gracedb.invalid'
    )
    tmp = {key: app.conf[key] for key in new_conf.keys()}
    app.conf.update(new_conf)
    yield
    app.conf.update(tmp)


@pytest.fixture(autouse=True)
def fake_gracedb_client(monkeypatch):
    mock_client = unittest.mock.create_autospec(rest.GraceDb)
    mock_client.service_url = 'https://gracedb.invalid/api/'
    monkeypatch.setattr('gwcelery.tasks.gracedb.client', mock_client)
    yield


def pytest_runtest_setup():
    disable_socket()
