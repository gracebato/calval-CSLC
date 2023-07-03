#!/usr/bin/env python3
import argparse
import pandas as pd
import datetime as dt
import itertools
import numpy as np
import os, time
import subprocess
from src.RLE_utils import hdf_stream, convert_to_slcvrt

def sort_pair(days,minDelta=5,maxDelta=90):
    '''
    days: list of days
    minDelta: minimum temporal baseline (days), maxDelta: maximum temporal baseline (days)
    
    determining pairs of reference and secondary dates
    given minimum and maximum temporal baseline
    '''
    date_pair = list(itertools.combinations(days,2))   #possible pair of InSAR dates

    refDates = []
    secDates = []
    deltas = []

    for refDate, secDate in date_pair:
        delta = dt.datetime.strptime(secDate, "%Y%m%d") - dt.datetime.strptime(refDate, "%Y%m%d")
        delta = int(delta.days)
        
        if (delta > minDelta) & (delta < maxDelta):
            refDates.append(refDate)
            secDates.append(secDate)
            deltas.append(delta)
    
    return refDates, secDates

def neighbor_pair(days,n_neighbor=2):
    '''
    days: list of days
    n_neighbor: number of neighboring pair
    
    determining pairs of reference and secondary dates
    given number of neighbor pairs
    '''
    refDates = []
    secDates = []
    ndays = len(days)

    for i,_ref in enumerate(days):
        _ = days[i+1:min(ndays,i+1+n_neighbor)]
    
        for _sec in _:
            refDates.append(_ref)
            secDates.append(_sec)
    
    return refDates, secDates

def createParser(iargs = None):
    '''Commandline input parser'''
    parser = argparse.ArgumentParser(description='pycuampcor offset tracking with CSLC products')
    parser.add_argument("--s3path", dest='s3path',
                         required=True, type=str, help='aws S3 bucket location (e.g., s3://opera-provisional/...)')
    parser.add_argument("--burstID", dest='bid',
                         required=True, type=str, help='burst ID to be processed')
    parser.add_argument("--datefile", dest='dfile',
                         required=True, type=str, help='text file with 1-column dates (YYYYMMDD) to be processed')
    parser.add_argument("--slc_dir", dest="slc_dir",
            default='SLCDIR', type=str, help='slc directory (default: SLCDIR)')
    parser.add_argument("--out_dir", dest="out_dir",
            default='outputs', type=str, help='output directory for offset results (default: outputs)')
    parser.add_argument("--neighbor", dest="neighbor",
            default=3, type=int, help='number of neighboring pairs (default: 3)')
    parser.add_argument("--ww", dest="ww",
            default=64, type=int, help='window width for offset tracking of pycuampcor (default: 64)')
    parser.add_argument("--wh", dest="wh",
            default=64, type=int, help='window height for offset tracking of pycuampcor (default: 64)')
    parser.add_argument("--nwdc", dest="nwdc",
            default=20, type=int, help='number of windows processed in a chunk along lines (default: 20)')
    parser.add_argument("--nwac", dest="nwac",
            default=20, type=int, help='number of windows processed in a chunk along columns (default: 20')

    return parser.parse_args(args=iargs)

def run(inps):

    data_dir = inps.s3path
    burst_id = inps.bid

    slc_dir = inps.slc_dir
    out_dir = inps.out_dir
    f = open(inps.dfile)
    datels = f.read().splitlines()

    n_neighbor = inps.neighbor

    ##parameters for pycuampcor
    windowSizeWidth = inps.ww     #window size (width) for pycuampcor
    windowSizeHeight = inps.wh    #window size (height) for pycuampcor

    #parameters for GPU parallel processing
    numberWindowDownInChunk = inps.nwdc     #The number of windows processed in a batch/chunk, along lines
    numberWindowAcrossInChunk = inps.nwac   #The number of windows processed in a batch/chunk, along columns

    #parameters for gpu processing
    num_gpu = subprocess.getoutput('nvidia-smi --list-gpus | wc -l')
    num_gpu = int(num_gpu)
    print(f'number of GPU: {num_gpu} \n')

    #generating neighboring pairs
    refDates, secDates = neighbor_pair(datels,n_neighbor=n_neighbor)

    summer_dates = []

    for _ in datels:
        _mon = int(_[4:6])

        #only including summer monthos between May and Oct
        if (_mon>=5) and (_mon<=10):
            summer_dates.append(_)

    #adding pairs with ~1 year temporal baseline
    _ref, _sec = sort_pair(summer_dates,minDelta=345,maxDelta=375)

    refDates = refDates + _ref
    secDates = secDates + _sec

    deltas = []
    gpuIDs = []
    _gpuID = 0

    for refDate, secDate in zip(refDates,secDates):

        delta = dt.datetime.strptime(secDate, "%Y%m%d") - dt.datetime.strptime(refDate, "%Y%m%d")
        delta = int(delta.days)
        deltas.append(delta)
        gpuIDs.append(_gpuID)
        _gpuID = _gpuID + 1
        _gpuID = _gpuID % num_gpu

    _ = {'ref': refDates, 'sec': secDates, 'deltaT':deltas, 'gpuID':gpuIDs}
    df = pd.DataFrame.from_dict(_)
    print('slc pairs to be processed:')
    print(df)

    days = refDates + secDates
    days = list(np.unique(sorted(days)))

    num_pairs = df.shape[0]   #number of pairs
    n_days = len(days)      #number of unique days

    print(f'number of pairs for offset tracking {num_pairs}\n')

    st_time = time.time()

    #creating slc inputs from COMPASS hdf file
    for day in days:
        outSLC = slc_dir + '/' + day + '.slc'
        outSLCvrt = outSLC + '.vrt'

        if os.path.isfile(outSLC) and (os.path.isfile(outSLCvrt)):
            print(f'{day}.slc exist. \n')
        else:
            path_h5 = f'{data_dir}/{burst_id}/{day}/{burst_id}_{day}.h5'   #path to COMPASS CSLC h5 file in aws s3 bucket

            xcoor, ycoor, dx, dy, epsg, slc, date = hdf_stream(path_h5)   
            convert_to_slcvrt(xcoor, ycoor, dx, dy, epsg, slc, date, slc_dir)   #generating slc with vrt 

    end_time = time.time()
    time_taken = (end_time - st_time)/60.
    print(f'{time_taken} min taken for streaming data')

    st_time = time.time()
    max_processes = num_gpu
    processes = set()

    #main offset tracking with pycuampcor
    for refd, secd, deviceID in zip(df['ref'],df['sec'],df['gpuID']):

        rgoff_file = out_dir + '/' + refd + '_' + secd + '.rg_off.tif'
        azoff_file = out_dir + '/' + refd + '_' + secd + '.az_off.tif'
        snr_file = out_dir + '/' + refd + '_' + secd + '.snr.tif'

        if os.path.isfile(rgoff_file) and os.path.isfile(azoff_file) and os.path.isfile(snr_file):
            print(f'{rgoff_file}, {azoff_file}, {snr_file} already exist \n')    #when files exist, skipping offset tracking
        else:
            cmd = f'python offset_pycuampcor.py --slc_dir {slc_dir} --dateref {refd} --datesec {secd} --deviceID {deviceID} --out_dir {out_dir} --ww {windowSizeWidth} --wh {windowSizeHeight} --nwdc {numberWindowDownInChunk} --nwac {numberWindowAcrossInChunk}'
            print(cmd)
            processes.add(subprocess.Popen(cmd.split(' ')))
            if len(processes) >= max_processes:
                os.wait()
                processes.difference_update([p for p in processes if p.poll() is not None])

    #Check if all the child processes were closed
    for p in processes:
        if p.poll() is None:
            p.wait()

    end_time = time.time()
    time_taken = (end_time - st_time)/60.
    print(f'{time_taken} min taken for all pycuampcor processing')

if __name__ == '__main__':
    # load arguments from command line
    inps = createParser()
    
    # Run workflow
    run(inps)
