"""Create mock events from the "Burst First Two Years" paper."""
import random
import json
import pkg_resources
import fileinput
import tempfile

from celery.task import PeriodicTask
from celery.utils.log import get_task_logger
from astropy.time import Time
from numpy.random import binomial

from ..import app
from . import gracedb

log = get_task_logger(__name__)


@app.task(shared=False)
def pick_bursts():

    # sample detectors with ball park duty cyle ~ .7
    detectors = random.sample(['L1', 'H1', 'V1', 'K1'],
                              max(2, binomial(4, .7)))
    # get current gps time
    gps_now = Time.now().gps

    # get static olib data file
    olib_file = pkg_resources.resource_filename(
        __name__, '../data/first2years_bursts/olib_data.json')

    with open(olib_file) as olib_json:
        # change file to reflect which instruments detected,
        # as well update gpstime
        olib_data = json.load(olib_json)
        olib_data['instruments'] = ",".join(detectors)
        olib_data['gpstime'] = gps_now
        olib_data = json.dumps(olib_data)

    # get static olib skymap
    olib_skymap_path = "../data/first2years_bursts/" \
                       "BF2Y_G0-LIB_C.fits.gz"

    # get static cwb data file
    cwb_file = pkg_resources.resource_filename(
          __name__, "../data/first2years_bursts/trigger_test.txt")

    # create temporary file 
    with tempfile.TemporaryFile("w+t") as f:
    # loop over static cwb file
    for line in fileinput.input(cwb_file):
        # replace 'time' line with correct current gps time
        if line.startswith('time:'):
            new_time_str = f'time:       {gps_now} {gps_now}'
            f.write(new_time_str)
        else:
            f.write(line)
    f.seek(0)
    cwb_data = f.read()

    # get static cwb skymap
    cwb_skymap_path = "../data/first2years_bursts/" \
                      "BF2Y_G0-cWB_C.fits.gz"
    return olib_data, olib_skymap_path, cwb_data, cwb_skymap_path


@app.task(shared=False)
def _vet_event(superevents):
    if superevents:
        gracedb.create_signoff.s(
            random.choice(['NO', 'OK']),
            'If this had been a real gravitational-wave event candidate, '
            'then an on-duty scientist would have left a comment here on '
            'data quality and the status of the detectors.',
            'ADV', superevents[0]['superevent_id']
        ).apply_async()


@gracedb.task(ignore_result=True, shared=False)
def _upload_skymap(graceid, skymap_path, pipeline):

    skymap = pkg_resources.resource_string(
              __name__, skymap_path)

    # upload skymap generated from corresponding pipeline
    gracedb.upload(skymap, f'{pipeline}.fits.gz', graceid,
                   f'{pipeline} skymap fits', ['sky_loc'])


@app.task(base=PeriodicTask, shared=False, run_every=3600)
def upload_events():
    """Upload a random event from the "Burst First Two Years" paper.

    After 2 minutes, randomly either retract or confirm the event to send a
    retraction or initial notice respectively.
    """
    olib_data, olib_skymap_path, cwb_data, cwb_skymap_path = pick_bursts()
    # pick pipelines that detected event
    # oLIB, CWB or both (right now always both)

    # for loop for multiple pipeline events
    for pipeline, burst_data, skymap_path in zip(
            ['oLIB', 'CWB'],
            [olib_data, cwb_data],
            [olib_skymap_path, cwb_skymap_path]):

        graceid = gracedb.create_event(burst_data, 'MDC', pipeline, 'Test')
        log.info('uploaded as %s', graceid)

        if app.conf['mock_events_simulate_multiple_uploads']:
            num = 3
            for _ in range(num):
                (
                    gracedb.create_event.s(
                        burst_data, 'MDC', pipeline, 'Test'
                    )
                    |
                    _upload_skymap.s(skymap_path, pipeline)

                ).apply_async()

        (
            _upload_skymap.si(graceid, skymap_path, pipeline)
            |
            gracedb.get_superevents.si(
                'Test event: {}'.format(graceid)
            ).set(countdown=600)
            |
            _vet_event.s()
        ).apply_async()

    # return altered files so all parameters match for unit testing purposes
    return olib_data, olib_skymap_path, cwb_data, cwb_skymap_path
