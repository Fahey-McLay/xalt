from __future__ import print_function
import os

def translate(nameA, envT, userT):
  sysT = {}
  queueType = "SLURM"
  if (envT.get("SGE_ACCOUNT")):
    queueType = "SGE"
  elif (envT.get("SLURM_TACC_ACCOUNT")):
    queueType = "SLURM_TACC"
  elif (envT.get("SBATCH_ACCOUNT")):
    queueType = "SLURM"
  elif (envT.get("PBS_JOBID")):
    queueType = "PBS"
      
  if (queueType == "SGE"):
    sysT['num_cores'] = "NSLOTS"
    sysT['num_nodes'] = "NHOSTS"
    sysT['account']   = "SGE_ACCOUNT"
    sysT['job_id']    = "JOB_ID"
    sysT['queue']     = "QUEUE"
      
  elif (queueType == "SLURM_TACC"):
    sysT['num_cores'] = "SLURM_TACC_CORES"
    sysT['num_nodes'] = "SLURM_NNODES"
    sysT['account']   = "SLURM_TACC_ACCOUNT"
    sysT['job_id']    = "SLURM_JOB_ID"
    sysT['queue']     = "SLURM_QUEUE"
  
  elif (queueType == "SLURM"):
    sysT['num_nodes']  = "SLURM_JOB_NUM_NODES"   # or SLURM_NNODES
    sysT['job_id']     = "SLURM_JOB_ID"
    sysT['queue']      = "SLURM_QUEUE"

  elif (queueType == "PBS"):
    sysT['num_cores'] = "PBS_NP"
    sysT['num_nodes'] = "PBS_NNODES"
    sysT['account']   = "PBS_ACCOUNT"
    sysT['job_id']    = "PBS_JOBID"
    sysT['queue']     = "PBS_QUEUE"
  
  for name in nameA:
    result = "unknown"
    key    = sysT.get(name)
    if (key):
      result = envT.get(key,"unknown")
    userT[name] = result
    
  # Compute number of total nodes for Generic SLURM.
  if (queueType == "SLURM"):
    userT['num_cores'] = envT.get("SLURM_NNODES",0) * envT.get("SLURM_CPUS_ON_NODE",0)
  
  keyA = [ 'num_cores', 'num_nodes' ]

  for key in keyA:
    if (userT[key] == "unknown"):
      userT[key] = 0
    else:
      userT[key] = int(userT[key])    
  
  if (userT['job_id'] == "unknown"):
    userT['job_id'] = envT.get('JOB_ID','unknown')



