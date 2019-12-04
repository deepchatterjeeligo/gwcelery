"""Create and upload Swift sky maps."""
import io
import healpy as hp
import numpy as np
from ligo.skymap.io import fits
from astropy import units as u
from astropy_healpix import pixel_resolution_to_nside

from ..import app
from . import gracedb
from . import ligo_fermi_skymaps
from . import skymaps


@app.task(shared=False)
def create_swift_skymap(ra, dec, error):
    """Create a single pixel sky map given an RA and dec.

    Parameters
    ----------
    ra : float
        right ascension
    dec: float
        declination
    error: float
        error radius

    """
    #  The error is typically 2-5 arcmin for BAT localizations
    #   Use min 2 arcmin to prevent runaway file size
    min_error = 2 * u.arcmin
    if error * u.deg < min_error:
        error_radius = min_error
    else:
        error_radius = error * u.deg

    #  Determine resolution such that there are at least
    #  1 pixel across the error radius
    nside = pixel_resolution_to_nside(error_radius, round='up')
    skymap = np.zeros(hp.nside2npix(nside))

    #  Find the one pixel the event can localized to
    ind = hp.pixelfunc.ang2pix(nside, ra, dec, lonlat=True)
    skymap[ind] = 1

    return skymap


@app.task(shared=False)
def create_upload_swift_skymap(event):
    """Create and upload single pixel Swift sky map using
    RA and dec information.

    Parameters
    ----------
    event : dict
        Dictionary of Swift external event

    """
    graceid = event['graceid']
    swift_filename = 'swift_skymap.fits.gz'
    try:
        filename = ligo_fermi_skymaps.get_external_skymap_filename(graceid)
        if swift_filename in filename:
            return
    except ValueError:
        pass

    ra = event['extra_attributes']['GRB']['ra']
    dec = event['extra_attributes']['GRB']['dec']
    error = event['extra_attributes']['GRB']['error_radius']
    skymap = create_swift_skymap(ra, dec, error)

    with io.BytesIO() as f:
        fits.write_sky_map(f, skymap, moc=True)
        skymap_data = f.getvalue()

    message = (
        'Mollweide projection of <a href="/api/events/{graceid}/files/'
        '{filename}">{filename}</a>').format(
            graceid=graceid, filename=swift_filename)

    (
        gracedb.upload.si(skymap_data,
                          swift_filename,
                          graceid,
                          'Sky map created from GCN RA and dec.',
                          ['sky_loc'])
        |
        skymaps.plot_allsky.si(skymap_data, ra=ra, dec=dec)
        |
        gracedb.upload.s('swift_skymap.png',
                         graceid,
                         message,
                         ['sky_loc'])
    ).delay()
