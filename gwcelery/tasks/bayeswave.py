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
import sys

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
#path to conda system-wide release of BayesWave
#bw_prefix = '/cvmfs/ligo-containers.opensciencegrid.org/lscsoft/conda/latest/envs/ligo-py37/bin/'
bw_prefix = '/cvmfs/ligo-containers.opensciencegrid.org/lscsoft/conda/latest/envs/ligo-py27/bin/'
pipepath = '/home/bence.becsy/O3/BW/bin/bayeswave_pipe'
#pipepath = bw_prefix + 'bayeswave_pipe'

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
    #TODO: separating jobs based on ifo setting, which we ideally should read from graceDB
    workdir = '/home/bence.becsy/O3/zero_lag/jobs/'+preferred_event_id
    
    #path we need to add to PYTHONPATH for bayeswave_pipe to work
    pypath_to_add = "/home/bence.becsy/O3/BW/lib/python2.7/site-packages"
    #pypath_to_add = "/cvmfs/ligo-containers.opensciencegrid.org/lscsoft/conda/latest/envs/ligo-py27/lib/python2.7/site-packages"
    
    # -- Set up call to pipeline -- Niter=1000 for very quick tests
    #Added "python2.7" before the call to force it to use python 2.7
    pipe_call = 'export PYTHONPATH={extra_path}:${{PYTHONPATH}}; python2.7 {pipepath} {inifile} \
    --workdir {workdir} \
    --graceID {graceid} \
    --gdb-playground \
    --condor-submit'.format(extra_path=pypath_to_add ,pipepath=pipepath, inifile=ini_name, workdir=workdir, graceid=preferred_event_id)

    print("Calling: " + pipe_call)

    # -- Call the pipeline!
    os.system(pipe_call)