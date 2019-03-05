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

import numpy as np

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
    ini_template = env.get_template('bayeswave.jinja2')
    
    event_info = gracedb.get_event(preferred_event_id)
    ifos = event_info['extra_attributes']['MultiBurst']['ifos'].split(',')
    
    #Hanford parameters
    H1_frame = u"'H1':'H1_HOFT_C02'"
    H1_channel = u"'H1':'H1:DCS-CALIB_STRAIN_C02'"
    H1_analyze = u"h1-analyze = H1:DMT-ANALYSIS_READY:1"
    #Livingston parameters
    L1_frame = u"'L1':'L1_HOFT_C02'"
    L1_channel = u"'L1':'L1:DCS-CALIB_STRAIN_C02'"
    L1_analyze = u"l1-analyze = L1:DMT-ANALYSIS_READY:1"
    #Virgo parameters
    V1_frame = u"'V1':'V1Online'"
    V1_channel = u"'V1':'V1:Hrec_hoft_16384Hz'"
    V1_analyze = u"v1-analyze = V1:ITF_SCIENCEMODE"
    
    ifo_list = u"['"
    ifo_list += u"','".join(ifos)
    ifo_list += u"']"
    
    frame_dict = u"{"
    frame_dict += u",".join([frame_name for ifo, frame_name in zip(["H1", "L1", "V1"],[H1_frame, L1_frame, V1_frame]) if ifo in ifos])
    frame_dict += u"}"
    
    channel_dict = u"{"
    channel_dict += u",".join([channel_name for ifo, channel_name in zip(["H1", "L1", "V1"],[H1_channel, L1_channel, V1_channel]) if ifo in ifos])
    channel_dict += u"}"
    
    analyze_list = u""
    analyze_list += u"\n".join([analyze_name for ifo, analyze_name in zip(["H1", "L1", "V1"],[H1_analyze, L1_analyze, V1_analyze]) if ifo in ifos])
    
    ini_settings = {
        'ifos': ifo_list,
        'frames': frame_dict,
        'channels': channel_dict,
        'analyzelist': analyze_list
    }
    #print it out for testing
    print(ini_template.render(ini_settings))
    
    return ini_template.render(ini_settings)
    

@app.task(shared=False)
def dag_prepare(workdir, ini_file, preferred_event_id, superevent_id):
    """Create a Condor DAG to run BayesWave on a given event.

    Parameters
    ----------
    workdir : str
        The path to a work directory where the DAG file exits
    ini_file : str
        The path to the ini file
    preferred_event_id : str
        The GraceDb ID of a target preferred event
    superevent_id : str
        The GraceDb ID of a target superevent

    Returns
    -------
    submit_file : str
        The path to the .sub file
    """

    #gracedb.upload.delay(
    gracedb.upload(
        filecontents=None, filename=None, graceid=superevent_id,
        message='"BayesWave launched"',
        tags='pe'
    )
    
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

    return workdir + '/' + preferred_event_id + '.dag.condor.sub'
    

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
        gracedb.upload.delay(
            filecontents=None, filename=None, graceid=superevent_id,
            message='Job was aborted.', tags='pe'
        )
    elif type(exc) is condor.JobFailed:
        gracedb.upload.delay(
            filecontents=None, filename=None, graceid=superevent_id,
            message='Job failed', tags='pe'
        )


@app.task(ignore_result=True, shared=False)
def clean_up(workdir):
    """Clean up a run directory.

    Parameters
    ----------
    rundir : str
        The path to a run directory where the DAG file exits
    """
    #shutil.rmtree(rundir)
    subprocess.run("cp -r " + workdir + " /home/bence.becsy/public_html/O3/zero_lag/jobs/", shell=True)


@app.task(ignore_result=True, shared=False)
def upload_result(workdir, preferred_event_id):
    """Upload a PE result

    Parameters
    ----------
    graceid : str
        The GraceDb ID.
    """
    webpage_dir = glob.glob("/home/bence.becsy/public_html/O3/zero_lag/jobs/" + preferred_event_id+ "/trigtime*/")[0]
    rundir = glob.glob(workdir + '/trigtime*/')[0]
    
    event_info = gracedb.get_event(preferred_event_id)
    IFO = event_info['extra_attributes']['MultiBurst']['ifos'].split(',')

    freq = []
    bandwidth = []
    duration = []

    dur_low = [0 for ifo in IFO]
    dur_high = [0 for ifo in IFO]
    dur_c = [0 for ifo in IFO]

    # Get frequency, bandwidth, duration info (duration on logscale)
    for ifo in range(len(IFO)):
       data = np.genfromtxt(rundir + 'tables/signal_mode_{ifo}.txt'.format(ifo=IFO[ifo]))
       freq.append(data[12])
       bandwidth.append(data[14])
       dur_low[ifo] = np.around(10**data[10,1],3)
       dur_high[ifo] = np.around(10**data[10,2],3)
       dur_c[ifo] = np.around(10**data[10,0],3)

    freq = np.around(freq,2)
    bandwidth = np.around(bandwidth,2)

    # Get Bayes factor +error info
    data = np.genfromtxt(rundir + 'evidence_stacked.dat')

    BSG = data[2]-data[1]
    BSN = data[2]-data[0]

    err_SG = math.sqrt(data[5]+data[4])
    err_SN = math.sqrt(data[5]+data[3])

    err_SG = round(err_SG,2)
    err_SN = round(err_SN,2)

    # Format information to be sent to gracedb
    gdbtable = '<table> \
    <tr><th colspan=2>BW parameter estimation</th></tr> \
    <tr><th>Median</th><th>90% CI lower bound</th>\
    <tr><td>frequency (Hz)</td><td align=right>{freq}</td></tr> \
    <tr><td>&nbsp;&nbsp;&nbsp;90% CI lower bound</td><td align=right>{freq}</td></tr> \
    <tr><td>&nbsp;&nbsp;&nbsp;90% CI lower bound</td><td align=right>{freq}</td></tr> \
    <tr><td>bandwidth (Hz)</td><td align=right>{bw}</td></tr> \
    <tr><td>Duration (s)</td><td align=right>{dur}</td></tr> \
    <tr><td>lnBSG</td><td align=right>{BSG}</td></tr> \
    <tr><td>lnBSN</td><td align=right>{BSN}</td></tr> \
    </table>'.format(freq=freq,bw=bandwidth,dur=duration,BSG=BSG,BSN=BSN)

    paramtable = '<table> \
    <tr><th colspan=4>BW parameter estimation</th></tr> \
    <tr><th colspan=2>Param</th><th>Median</th><th>&nbsp;&nbsp;&nbsp;90%CI lower bound</th><th>&nbsp;&nbsp;&nbsp;90%CI upper bound</th></tr> \
    <tr><td rowspan=2>frequency (Hz)</td><td>H1</td><td align=right>{freqH}</td><td align=right>{freqHlow}</td><td align=right>{freqHhigh}</td></tr> \
    <tr><td>L1</td><td align=right>{freqL}</td><td align=right>{freqLlow}</td><td align=right>{freqLhigh}</td></tr> \
    <tr><td rowspan=2>bandwidth (Hz)</td><td>H1</td><td align=right>{bwH}</td><td align=right>{bwHlow}</td><td align=right>{bwHhigh}</td></tr> \
    <tr><td>L1</td><td align=right>{bwL}</td><td align=right>{bwLlow}</td><td align=right>{bwLhigh}</td></tr> \
    <tr><td rowspan=2>duration (s)</td><td>H1</td><td align=right>{durH}</td><td align=right>{durHlow}</td><td align=right>{durHhigh}</td></tr> \
    <tr><td>L1</td><td align=right>{durL}</td><td align=right>{durLlow}</td><td align=right>{durLhigh}</td></tr></table> \
    '.format(freqH=freq[0][0],freqHlow=freq[0][1],freqHhigh=freq[0][2],freqL=freq[1][0],freqLlow=freq[1][1],freqLhigh=freq[1][2],bwH=bandwidth[0][0],bwHlow=bandwidth[0][1],bwHhigh=bandwidth[0][2],bwL=bandwidth[1][0],bwLlow=bandwidth[1][1],bwLhigh=bandwidth[1][2],durH=dur_c[0],durL=dur_c[1],durHlow=dur_low[0],durLlow=dur_low[1],durHhigh=dur_high[0],durLhigh=dur_high[1])

    BFtable = '<table> \
    <tr><th colspan=2>BW Bayes Factors</th></tr> \
    <tr><td>lnBSG</td><td align=right>{BSG}+/-{errBSG}</td></tr> \
    <tr><td>lnBSN</td><td align=right>{BSN}+/-{errBSN}</td></tr> \
    </table>'.format(BSG=BSG,BSN=BSN,errBSG=err_SG,errBSN=err_SN)

    #print(gdbtable)
    #print(paramtable)
    #print(BFtable)

    # Sky map
    skyname = glob.glob(rundir + 'skymap*.fits')[0]

    os.system('cp {sky} {rundir}/BayesWave.fits'.format(sky=skyname, rundir=rundir)) # (change name so it's clear on GraceDB which skymap is ours)
    os.system('gzip {rundir}/BayesWave.fits'.format(rundir=rundir))

    #skytag = ["sky_loc","lvem"]
    skytag = ["sky_loc"]
    
    #I have no idea what "contents" is --  let's just put an empty string in there for now
    contents = ""
    # Actually send info to gracedb
    gracedb.upload(filecontents=contents, filename=rundir + 'BayesWave.fits.gz', graceid=preferred_event_id,
                   message="BayesWave skymap FITS", tags = skytag)
    gracedb.upload(filecontents=None, filename=None, graceid=preferred_event_id,
                   message="<a href='https://ldas-jobs.ligo.caltech.edu/~bence.becsy/{0}'>BW Follow-up results</a>".format('/'.join(webpage_dir.split('/')[4:])), tags='pe')
    gracedb.upload(filecontents=None, filename=None, graceid=preferred_event_id,
                   message=paramtable, tags='pe')
    gracedb.upload(filecontents=None, filename=None, graceid=preferred_event_id,
                   message=BFtable, tags='pe')


def dag_finished(workdir, preferred_event_id, superevent_id):
    """Upload BayesWave PE results and clean up run directory

    Parameters
    ----------
    rundir : str
        The path to a run directory where the DAG file exits
    preferred_event_id : str
        The GraceDb ID of a target preferred event
    superevent_id : str
        The GraceDb ID of a target superevent

    Returns
    -------
    tasks : canvas
        The work-flow for uploading PE results
    """

    return group(
        gracedb.upload.si(
            filecontents=None, filename=None, graceid=superevent_id,
            message='BayesWave online parameter estimation finished.',
            tags='pe'
        ),
        upload_result.si(
            workdir, preferred_event_id
        )
    ) | clean_up.si(workdir)


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
    
    """
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

        
    (
        dag_prepare.s(workdir, ini_file, preferred_event_id, superevent_id)
        |
        condor.submit.s().on_error(job_error_notification.s(superevent_id))
        |
        dag_finished(workdir, preferred_event_id, superevent_id)
    ).delay()
"""