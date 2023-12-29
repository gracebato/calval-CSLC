#!/usr/bin/env python3

import argparse
import os
import warnings
from pathlib import Path
import datetime as dt

import pandas as pd
import papermill as pm
import concurrent.futures
import timeit
warnings.filterwarnings('ignore')

def createParser(iargs = None):
    '''Commandline input parser'''
    parser = argparse.ArgumentParser(description='Correlate two IFGs with ON/OFF Azimuth FM rate correction')
    parser.add_argument("--ifglist", dest="ifglist",
                         required=True, type=str, help="Path to the CSV filelist of IFGs in s3")
    parser.add_argument("--savedir", dest='savedir',
                         required=True, type=str, help='Save directory')
    parser.add_argument("--nprocs", dest="nprocs",
                         default=2, type=int, help='Number of processes to run (default: 2)')
    return parser.parse_args(args=iargs)

def run_papermill(p):
    bucket_name = f"{p[0].split('/')[0]}/{p[0].split('/')[1]}/{p[0].split('/')[2]}"
    folder_path = f"{p[0].split('/')[3]}/{p[0].split('/')[4]}/{p[0].split('/')[5]}"
    aoi = p[0].split('/')[-3]
    ifg_fn = p[0].split('/')[-1]
    object_path_OFF = f"{folder_path}/no_az_fm_rate/{aoi}/ifg/{ifg_fn}"
    object_path_ON = f"{folder_path}/with_az_fm_rate/{aoi}/ifg/{ifg_fn}"

    # Create save directories
    savedir = f"{p[-1]}/{aoi}"
    os.makedirs(f"{savedir}/ipynbs/", exist_ok=True)
    os.makedirs(f"{savedir}/pngs/", exist_ok=True)
    os.makedirs(f"{savedir}/summary/", exist_ok=True)
    print(f"Saving all files to: {savedir}")
    
    # Run the ALE for each date via papermill
    pm.execute_notebook(f"inspect_ifgs_template.ipynb",
                f"{savedir}/ipynbs/inspect_ifgs_template_{aoi}_{object_path_OFF.split('/')[-1][6:23]}.ipynb",
                parameters={'savedir': savedir,
                            'bucket_name': bucket_name,
                            'object_path_OFF': object_path_OFF,
                            'object_path_ON':object_path_ON,
                            'aoi': aoi},
                kernel_name='calval_CSLC')
    
    return (f"Finished processing {aoi}_{object_path_OFF.split('/')[-1][6:23]}")

def main(inps):
    # Specify valid burst(s)
    # Default is to loop through all
    ifglist = inps.ifglist
    savedir = inps.savedir
    nprocs = inps.nprocs

    print(ifglist)
    # read list of ifglist 
    if os.path.isfile(ifglist)==True:
        ifglist_df = pd.read_csv(ifglist)
        ifglist_df[1] = savedir
    else:
        raise Exception(f'File not found.')

    # Start runtime evaluation
    start = timeit.default_timer()
    
    # Get all the filelist
    params = ifglist_df.values.tolist()
    
    print(f'Number of CPUs your computer have: {os.cpu_count()}')
    print(f'Using {nprocs} CPUs for this processing.')
    # Run papermill
    with concurrent.futures.ProcessPoolExecutor(max_workers=nprocs) as executor:
        for result in executor.map(run_papermill,params):
            print(result)

    # End runtime evaluation
    stop = timeit.default_timer()
    print('=================================================')
    print(f"Runtime:  {(stop - start)/60, min}")
    print('=================================================')

if __name__ == '__main__':
    # load arguments from command line
    inps = createParser()

    print("IFG Inspection Running")
    
    # Run the main function
    main(inps)