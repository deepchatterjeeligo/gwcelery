from unittest.mock import patch

from ..tasks import ligo_swift_skymaps


def test_create_swift_skymap():
    """Test created single pixel sky maps for Swift localization."""
    ra, dec, error = 0, 90, 0
    skymap = ligo_swift_skymaps.create_swift_skymap(ra, dec, error)
    assert skymap[0] == 1

    ra, dec, error = 270, -90, .1
    skymap = ligo_swift_skymaps.create_swift_skymap(ra, dec, error)
    assert skymap[-1] == 1


@patch('gwcelery.tasks.gracedb.upload.run')
@patch('gwcelery.tasks.skymaps.plot_allsky.run')
def test_create_upload_swift_skymap(mock_plot_allsky,
                                    mock_upload):
    """Test the creation and upload of sky maps for Swift localization."""
    event = {'graceid': 'E1234',
             'extra_attributes': {
                 'GRB': {
                     'ra': 0,
                     'dec': 0,
                     'error_radius': 0}}}
    ligo_swift_skymaps.create_upload_swift_skymap(event)
    mock_upload.assert_called()
    mock_plot_allsky.assert_called_once()
