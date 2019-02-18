"""Follow-up analysis with BayesWave."""
from distutils.spawn import find_executable
from distutils.dir_util import mkpath
import glob
import json
import math
import os
import shutil
import subprocess
import tempfile

from celery import group
from glue.lal import Cache
from gwdatafind import find_urls
from gwpy.timeseries import StateVector
import lal
import lalsimulation

from .. import app
from ..jinja import env
from . import condor
from . import gracedb


ini_name = '/home/bence.becsy/O3/zero_lag/zero_lag.ini'
pipepath = '/home/bence.becsy/O3/BW/bin/bayeswave_pipe'
bw_user_env_file = '/home/bence.becsy/O3/BW/etc/bayeswave-user-env.sh'


@app.task(ignore_result=True, shared=False)
def start_bayeswave(preferred_event_id, superevent_id):
    """Run BayesWave on a given event.

    Parameters
    ----------
    ini_contents : str
        The content of online_pe.ini
    preferred_event_id : str
        The GraceDb ID of a target preferred event
    superevent_id : str
        The GraceDb ID of a target superevent
    """
    # make a run directory
    workdir = '/home/bence.becsy/O3/zero_lag/jobs/'+preferred_event_id
    
    #make sure the environment is set for the run
    os.system(bw_user_env_file)
    
    # -- Set up call to pipeline -- Niter=1000 for very quick tests
    pipe_call = '{pipepath} {inifile} \
    --workdir {workdir} \
    --graceID {graceid} \
    --Niter 1000 \
    --condor-submit'.format(pipepath=pipepath, inifile=ini_name, workdir=workdir, graceid=preferred_event_id)
    
    print(pipe_call)