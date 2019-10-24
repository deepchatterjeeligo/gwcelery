import pkg_resources

from unittest.mock import call, patch
import pytest

from ..tasks.first2years_bursts import upload_events
from ..import app
pytest.importorskip('lal')


@patch('gwcelery.tasks.gracedb.create_event', return_value='M1234')
@patch('gwcelery.tasks.gracedb.upload.run')
@patch('gwcelery.tasks.gracedb.get_superevents.run',
       return_value=[{'superevent_id': 'S1234'}])
@patch('gwcelery.tasks.gracedb.create_signoff.run')
def test_upload_burst_events(mock_create_signoff, mock_get_superevents,
                             mock_upload, mock_create_event):
    app.conf['mock_events_simulate_multiple_uploads'] = True
    # get altered files from upload_events call
    olib_data, olib_skymap_path, cwb_data, cwb_skymap_path = upload_events()

    # assert gracedb.create_event called with proper calls
    mock_create_event.assert_has_calls([call(olib_data, 'MDC', 'oLIB', 'Test'),
                                        call(cwb_data, 'MDC', 'CWB', 'Test')],
                                       any_order=True)

    olib_skymap = pkg_resources.resource_string(
        __name__, olib_skymap_path)
    cwb_skymap = pkg_resources.resource_string(
        __name__, cwb_skymap_path)
    # assert gracedb.upload called with proper calls

    mock_upload.assert_has_calls([
        call(olib_skymap, 'oLIB.fits.gz', 'M1234',
             'oLIB skymap fits', ['sky_loc']),
        call(cwb_skymap, 'CWB.fits.gz', 'M1234',
             'CWB skymap fits', ['sky_loc'])], any_order=True)

    # assert get_superevents called with proper call
    mock_get_superevents.assert_called_with('Test event: M1234')

    msg = ('If this had been a real gravitational-wave event candidate, '
           'then an on-duty scientist would have left a comment here on '
           'data quality and the status of the detectors.')
    assert mock_create_signoff.call_args in (
        call('NO', msg, 'ADV', 'S1234'), call('OK', msg, 'ADV', 'S1234'))
