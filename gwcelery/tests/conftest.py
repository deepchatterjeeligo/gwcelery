import pytest

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
        lvalert_host='lvalert.invalid'
    )
    tmp = {key: app.conf[key] for key in new_conf.keys()}
    app.conf.update(new_conf)
    yield
    app.conf.update(tmp)