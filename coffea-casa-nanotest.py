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
from distributed.security import Security

#'ZJets': ['/mnt/hadoop/user/uscms01/pnfs/unl.edu/data4/cms/store/user/oshadura/nano_dy.root'],
#'Data' : ['/mnt/hadoop/user/uscms01/pnfs/unl.edu/data4/cms/store/user/oshadura/nano_dimuon.root']
filelist = {
        'ZJets': ['data/nano_dy.root'],
        'Data' : ['data/nano_dimuon.root']
        }
treename = 'Events'
compression = 2

#Coffea test processor
proc = NanoTestProcessor()

sec_dask = Security(tls_ca_file='/etc/cmsaf-secrets/ca.pem',
               tls_worker_cert='/etc/cmsaf-secrets/usercert.pem',
               tls_worker_key='/etc/cmsaf-secrets/userkey.pem',
               tls_scheduler_cert='/etc/cmsaf-secrets/hostcert.pem',
               tls_scheduler_key='/etc/cmsaf-secrets/hostkey.pem',
               require_encryption=True)

cluster = HTCondorCluster(cores=4,
                          memory="2GB",
                          disk="1GB",
                          log_directory="logs",
                          silence_logs="debug",
                          scheduler_options= {"dashboard_address":"8786","port":8787, "external_address": "129.93.183.33:8787"},
                          # HTCondor submit script
                          job_extra={"universe": "docker",
                                     # To be used with coffea-casa:0.1.4
                                     "encrypt_input_files": "/etc/cmsaf-secrets/xcache_token",
                                     #"docker_network_type": "host",
                                     "docker_image": "oshadura/coffea-casa-analysis:0.1.6", 
                                     "container_service_names": "dask",
                                     "dask_container_port": "8787",
                                     "should_transfer_files": "YES",
                                     "when_to_transfer_output": "ON_EXIT",
                                     "+DaskSchedulerAddress": '"129.93.183.33:8787"',
                                    })

cluster.scale(jobs=1)

client = Client(cluster)#, security=sec_dask)

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
