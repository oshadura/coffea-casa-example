from __future__ import division, print_function

import os
import os.path as osp
import sys
import warnings

import coffea.processor as processor
import numpy as np
from coffea import hist
from coffea import processor as processor
from coffea.analysis_objects import JaggedCandidateArray
from coffea.processor.test_items import NanoTestProcessor
from dask.distributed import Client, LocalCluster
from dask_jobqueue import HTCondorCluster

treename = 'Events'
compression = 2


#'ZJets': ['/mnt/hadoop/user/uscms01/pnfs/unl.edu/data4/cms/store/user/oshadura/nano_dy.root'],
#'Data' : ['/mnt/hadoop/user/uscms01/pnfs/unl.edu/data4/cms/store/user/oshadura/nano_dimuon.root']
filelist = {
        'ZJets': ['data/nano_dy.root'],
        'Data' : ['data/nano_dimuon.root']
        }

treename = 'Events'
compression = 2

proc = NanoTestProcessor()

cluster = HTCondorCluster(cores=2, memory="2GB", disk="1GB", log_directory="logs", silence_logs="debug", scheduler_options={"dashboard_address":"9998"}, job_extra={"universe": "docker"," docker_image": "oshadura/coffea-casa:latest", "container_service_names": "condor", "condor_container_port": "8787", "should_transfer_files": "YES", "when_to_transfer_output": "ON_EXIT"})
cluster.scale(jobs=1)
client = Client(cluster)

print("Dask client: ", client)

exe_args = {
        'client': client,
        'compression': compression,
}

hists = processor.run_uproot_job(filelist,
                                 treename,
                                 processor_instance=proc,
                                 executor=processor.dask_executor,
                                 #executor=processor.futures_executor,
                                 executor_args=exe_args)

assert( hists['cutflow']['ZJets_pt'] == 18 )
assert( hists['cutflow']['ZJets_mass'] == 6 )
assert( hists['cutflow']['Data_pt'] == 84 )
assert( hists['cutflow']['Data_mass'] == 66 )
