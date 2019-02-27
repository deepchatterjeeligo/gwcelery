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

pipepath = '/home/bence.becsy/O3/BW/bin/bayeswave_pipe'

@app.task(shared=False)
def prepare_ini(preferred_event_id, superevent_id=None):
    """Determine an appropriate PE settings for the target event and return ini
    file content
    """
    ini_file = os.getenv('HOME') + '/public_html/O3/BayesWave/bayes_wave_zero_lag_{0}.ini'.format(preferred_event_id)
    template = """[input]
dataseed=1234
PSDlength=64.0
padding=0.0
ifo-list={0}

[engine]
bayeswave=/cvmfs/ligo-containers.opensciencegrid.org/lscsoft/conda/latest/envs/ligo-py27/bin/BayesWave
bayeswave_post=/cvmfs/ligo-containers.opensciencegrid.org/lscsoft/conda/latest/envs/ligo-py27/bin/BayesWavePost
megaplot=/cvmfs/ligo-containers.opensciencegrid.org/lscsoft/conda/latest/envs/ligo-py27/bin/megaplot.py
megasky=/cvmfs/ligo-containers.opensciencegrid.org/lscsoft/conda/latest/envs/ligo-py27/bin/megasky.py

[datafind]
frtype-list={1}
channel-list={2}
url-type=file
veto-categories=[1]

[bayeswave_options]
bayesLine=
updateGeocenterPSD=
waveletPrior=
Dmax=100
#quick runs for gwcelery testing -- TODO: REMOVE THIS FOR PRODUCTION
Niter=500

[bayeswave_post_options]
0noise=
bayesLine=

[condor]
universe=vanilla
checkpoint=
bayeswave-request-memory=300
bayeswave_post-request-memory=2000
datafind=/usr/bin/gw_data_find
ligolw_print=/usr/bin/ligolw_print
segfind=/usr/bin/ligolw_segment_query_dqsegdb
accounting_group = ligo.prod.o3.burst.paramest.bayeswave

[segfind]
segment-url=https://segments.ligo.org

[segments]
{3}
{4}
"""
    with open(ini_file, 'w') as i_f:
        i_f.write(template.format("['H1','V1']", "{'H1':'H1_HOFT_C02','V1':'V1Online'}",
                                 "{'H1':'H1:DCS-CALIB_STRAIN_C02','V1':'V1:Hrec_hoft_16384Hz'}",
                                 "h1-analyze = H1:DMT-ANALYSIS_READY:1",
                                 "v1-analyze = V1:ITF_SCIENCEMODE"))
    return ini_file
    
@app.task(ignore_result=True, shared=False)
def start_bayeswave(preferred_event_id, superevent_id, gdb_playground=False):
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
    workdir = os.getenv('HOME') + '/public_html/O3/BayesWave/jobs/{0}'.format(preferred_event_id)
    
    ini_file = prepare_ini(preferred_event_id)
    
    #path we need to add to PYTHONPATH for bayeswave_pipe to work
    #pypath_to_add = "/home/bence.becsy/O3/BW/lib/python2.7/site-packages"
    pypath_to_add = "/cvmfs/ligo-containers.opensciencegrid.org/lscsoft/conda/latest/envs/ligo-py27/lib/python2.7/site-packages"
    
    # -- Set up call to pipeline -- 
    #Added "python2.7" before the call to force it to use python 2.7
    if gdb_playground:
        pipe_call = 'conda; export PYTHONPATH={extra_path}:${{PYTHONPATH}}; python2.7 {pipepath} {inifile} \
        --workdir {workdir} \
        --graceID {graceid} \
        --gdb-playground \
        --condor-submit'.format(extra_path=pypath_to_add ,pipepath=pipepath, inifile=ini_file, workdir=workdir, graceid=preferred_event_id)
    else:
        #test for O2 replay data -- needs trigger time
        pipe_call = 'export PYTHONPATH={extra_path}:${{PYTHONPATH}}; python2.7 {pipepath} {inifile} \
        --workdir {workdir} \
        --trigger-time 1187051080.46 \
        --condor-submit'.format(extra_path=pypath_to_add ,pipepath=pipepath, inifile=ini_file, workdir=workdir)

    print("Calling: " + pipe_call)

    # -- Call the pipeline!
    os.system(pipe_call)