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

@app.task(ignore_result=True, shared=False)
def job_error_notification(request, exc, traceback, superevent_id):
    """Upload notification when condor.submit terminates unexpectedly.

    Parameters
    ----------
    request : Context (placeholder)
        Task request variables
    exc : Exception
        Exception rased by condor.submit
    traceback : str (placeholder)
        Traceback message from a task
    superevent_id : str
        The GraceDb ID of a target superevent
    """
    if type(exc) is condor.JobAborted:
        #gracedb.upload.delay(
        gracedb.upload(
            filecontents=None, filename=None, graceid=superevent_id,
            message='Job was aborted.', tags='pe'
        )
    elif type(exc) is condor.JobFailed:
        #gracedb.upload.delay(
        gracedb.upload(
            filecontents=None, filename=None, graceid=superevent_id,
            message='Job failed', tags='pe'
        )

@app.task(shared=False)
def prepare_ini(preferred_event_id, superevent_id=None):
    """Determine an appropriate PE settings for the target event and return ini
    file content
    """
    ini_file = os.getenv('HOME') + '/public_html/O3/BayesWave/bayes_wave_zero_lag_{0}.ini'.format(preferred_event_id)
    template = """[input]
dataseed=1234

; variable srate (use min-srate for trigger<frequency-threshold; max-srate for
; trigger>frequency-threshold)
frequency-threshold=200
min_srate=4096
max_srate=4096
max-seglen=2
min-seglen=2
;if no trigger frequency is found, this value is used:
flow=16.0
; threshold for setting flow (and srate, window length and seglen):
frequency_threshold = 200.0
; if trigger_frequency < 200.0: flow = min_flow
min_flow=16.0
; else: flow = max_flow
max_flow = 64.0

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
"""

    event_info = gracedb.get_event(preferred_event_id)
    ifos = event_info['extra_attributes']['MultiBurst']['ifos'].split(',')
    
    #Hanford parameters
    H1_frame = "'H1':'H1_HOFT_C02'"
    H1_channel = "'H1':'H1:DCS-CALIB_STRAIN_C02'"
    H1_analyze = "h1-analyze = H1:DMT-ANALYSIS_READY:1"
    #Livingston parameters
    L1_frame = "'L1':'L1_HOFT_C02'"
    L1_channel = "'L1':'L1:DCS-CALIB_STRAIN_C02'"
    L1_analyze = "l1-analyze = L1:DMT-ANALYSIS_READY:1"
    #Virgo parameters
    V1_frame = "'V1':'V1Online'"
    V1_channel = "'V1':'V1:Hrec_hoft_16384Hz'"
    V1_analyze = "v1-analyze = V1:ITF_SCIENCEMODE"
    
    ifo_list = "['"
    ifo_list += "','".join(ifos)
    ifo_list += "']"
    
    frame_dict = "{"
    frame_dict += ",".join([frame_name for ifo, frame_name in zip(["H1", "L1", "V1"],[H1_frame, L1_frame, V1_frame]) if ifo in ifos])
    frame_dict += "}"
    
    channel_dict = "{"
    channel_dict += ",".join([channel_name for ifo, channel_name in zip(["H1", "L1", "V1"],[H1_channel, L1_channel, V1_channel]) if ifo in ifos])
    channel_dict += "}"
    
    analyze_list = ""
    analyze_list += "\n".join([analyze_name for ifo, analyze_name in zip(["H1", "L1", "V1"],[H1_analyze, L1_analyze, V1_analyze]) if ifo in ifos])
    
    with open(ini_file, 'w') as i_f:
        i_f.write(template.format(ifo_list, frame_dict, channel_dict, analyze_list))
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
    pypath_to_add = "/home/bence.becsy/O3/BW/lib/python2.7/site-packages"
    #pypath_to_add = "/cvmfs/ligo-containers.opensciencegrid.org/lscsoft/conda/latest/envs/ligo-py27/lib/python2.7/site-packages/bayeswave_pipe:/cvmfs/ligo-containers.opensciencegrid.org/lscsoft/conda/latest/envs/ligo-py27/lib/python2.7/site-packages/bayeswave_pipe_examples:/cvmfs/ligo-containers.opensciencegrid.org/lscsoft/conda/latest/envs/ligo-py27/lib/python2.7/site-packages/bayeswave_plot:/cvmfs/ligo-containers.opensciencegrid.org/lscsoft/conda/latest/envs/ligo-py27/lib/python2.7/site-packages/bayeswave_plot_data"
    
    # -- Set up call to pipeline -- 
    #Added "python2.7" before the call to force it to use python 2.7
    if gdb_playground:
        pipe_call = 'export PYTHONPATH={extra_path}:${{PYTHONPATH}}; python2.7 {pipepath} {inifile} \
        --workdir {workdir} \
        --graceID {graceid} \
        --gdb-playground'.format(extra_path=pypath_to_add ,pipepath=pipepath, inifile=ini_file, workdir=workdir, graceid=preferred_event_id)
    else:
        #test for O2 replay data -- needs trigger time because there are no frames for the actual time
        pipe_call = 'export PYTHONPATH={extra_path}:${{PYTHONPATH}}; python2.7 {pipepath} {inifile} \
        --workdir {workdir} \
        --trigger-time 1187051080.46'.format(extra_path=pypath_to_add ,pipepath=pipepath, inifile=ini_file, workdir=workdir)

    #print("Calling: " + pipe_call)

    #gracedb.upload.delay(
    gracedb.upload(
        filecontents=None, filename=None, graceid=superevent_id,
        message='"BayesWave launched"',
        tags='pe'
    )
    
    # -- Call the pipeline!
    #os.system(pipe_call)
    try:
        subprocess.run(pipe_call, shell=True,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                       check=True)
        subprocess.run(['condor_submit_dag', '-no_submit',
                        workdir + '/' + preferred_event_id + '.dag'],
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                       check=True)
    except subprocess.CalledProcessError as e:
        contents = b'args:\n' + json.dumps(e.args[1]).encode('utf-8') + \
                   b'\n\nstdout:\n' + e.stdout + b'\n\nstderr:\n' + e.stderr
        #gracedb.upload.delay(
        gracedb.upload(
            filecontents=contents, filename='pe_dag.log',
            graceid=superevent_id,
            message='Failed to prepare DAG', tags='pe'
        )
        raise
    
    condor.submit.s(workdir + '/' + preferred_event_id + '.dag.condor.sub').on_error(job_error_notification.s(superevent_id))