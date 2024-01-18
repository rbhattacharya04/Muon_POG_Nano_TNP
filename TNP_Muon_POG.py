#!/usr/bin/env python

import ROOT
from sys import exit
from sys import argv
import argparse
import time
import json
from Muon_TnP_cfg import *
import os
from pathlib import Path
import subprocess
import sys
import requests
sys.path.insert(0, list(filter(lambda k: 'myenv' in k, sys.path))[0])
import uproot
import awkward as ak
import pandas as pd
import numpy as np

from math import ceil

ROOT.gInterpreter.ProcessLine(".O3")
#ROOT.EnableImplicitMT(1)                                  ### Not possible with the original approach: df.Range() to write in chunks
ROOT.gInterpreter.Declare('#include "headers.hh"')
ROOT.gInterpreter.Declare('#include "TNP_Muon_POG.h"') 


############################################################ 
################### MUON POG ANALYSIS ######################
##################### TAG AND PROBE ########################
####################### nanoAOD ############################
############################################################

def defaultParser():
    parser = argparse.ArgumentParser(add_help=False)

    parser.add_argument(
        "-p",
        "--prod",
        type=str,
        help="Production name to run",
        required=True,
    )

    parser.add_argument(
        "-s",
        "--doSubmit",
        type=int,
        choices=[0, 1],
        help="1: split and submit jobs to condor; 0: run interactively",
        required=False,
        default=0,
    )

    parser.add_argument(
        "-F",
        "--selFile",
        type=str,
        help="File to run",
        required=False,
        default="",
    )

    parser.add_argument(
        "-dR",
        "--dryRun",
        choices=[0, 1],
        type=int,
        help="1 do not submit to condor",
        required=False,
        default=0,
    )

    return parser
    
def create_TnP_pairs(era, fileName=""):
    
    start = time.time()

    print("Start computation!")

    name_tag = ""
    if fileName=="":
        #### Read input files
        files = []
        cmd = "find {} -wholename '*/*.root'".format(samples[era]["input"])
        fnames = subprocess.check_output(cmd, shell=True).strip().split(b'\n')
        files = files + [fname.decode('ascii') for fname in fnames]
    else:
        files = [fileName]
        name_tag = fileName.split("NanoMuonPOGData_")[1].split(".root")[0]
            
    print(files[0:10])

    filenames = ROOT.std.vector('string')()
    for name in files: filenames.push_back(name)

    print(filenames)

    print("Creating datframe")
    df = ROOT.RDataFrame("Events",filenames)

    print("Number of events -> " + str(df.Count().GetValue()))

    # Apply lumi mask
    lumiFile = samples[era]["lumiMask"]
    lumiText = requests.get(lumiFile)
    lumiJson = json.loads(lumiText.text)
    filters = []
    for run, lumiRanges in lumiJson.items():
        subFilters = []
        for lumiRange in lumiRanges:
            subFilters.append(
                f"( luminosityBlock >= {lumiRange[0]} && luminosityBlock <= {lumiRange[1]} )"
            )
        subFiltersMerged = " || ".join(subFilters)
        filters.append(f"( run == {run} && ( {subFiltersMerged} ) )")    
    total_filter = " || ".join(filters)
    #print(total_filter)
    df = df.Filter(total_filter)
    
    # just for utility
    df = df.Alias("Tag_pt",  "Muon_pt")
    df = df.Alias("Tag_eta", "Muon_eta")
    df = df.Alias("Tag_phi", "Muon_phi")
    df = df.Alias("Tag_charge", "Muon_charge")
    
    
    ### Match between TrigObj and Muon collections
    df = df.Define("Muon_trigIdx", "CreateTrigIndex(Muon_eta, Muon_phi, TrigObj_eta, TrigObj_phi, 0.1)")
    df = df.Define("Muon_isTrig", "Muon_trigIdx < 950")
    df = df.Define("Muon_filterBits", "Take(TrigObj_filterBits, Muon_trigIdx, 0)")
    df = df.Define("Muon_HLTIsoMu24", "(Muon_filterBits & 8) > 0")
    
    df = df.Alias("Tag_trigIdx", "Muon_trigIdx")
    df = df.Alias("Tag_isTrig", "Muon_isTrig")
    df = df.Alias("Tag_HLTIsoMu24", "Muon_HLTIsoMu24")
    
    ### Apply selection
    df = df.Define("Tag_Muons",   selection["tag"])
    df = df.Define("Probe_Muons", selection["probe"])
    df = df.Define("All_TPPairs", "CreateTPPair(Tag_Muons, Probe_Muons, 1, Tag_charge, Muon_charge, 1)")
    df = df.Define("All_TPmass","getTPVariables(All_TPPairs, Tag_pt, Tag_eta, Tag_phi, Muon_pt, Muon_eta, Muon_phi, 4)")
    
    massLow  =  60
    massHigh = 120
    
    massCut = f"All_TPmass > {massLow} && All_TPmass < {massHigh}"
    
    df = df.Define("TPPairs", "All_TPPairs[%s]" % selection["pair"])
    df = df.Filter("TPPairs.size() > 0")
    
    df = df.Define("pair_mass",  "All_TPmass[%s]" % selection["pair"])
    df = df.Define("pair_pt","getTPVariables(TPPairs, Tag_pt, Tag_eta, Tag_phi, Muon_pt, Muon_eta, Muon_phi, 1)")
    df = df.Define("pair_eta","getTPVariables(TPPairs, Tag_pt, Tag_eta, Tag_phi, Muon_pt, Muon_eta, Muon_phi, 2)")
    df = df.Define("pair_phi","getTPVariables(TPPairs, Tag_pt, Tag_eta, Tag_phi, Muon_pt, Muon_eta, Muon_phi, 3)")

    df = df.Define("npairs", "TPPairs.size()")
    df = df.Define("nTag", "Sum(Tag_Muons==true)")
    df = df.Define("probe_isDuplicated", "getDuplicatedProbes(TPPairs, Muon_pt)")
    
    ### Create probe branches
    for var in variables["probe"]:
        df = df.Define("probe_"+var, "getVariables(TPPairs, Muon_"+var+", 2)")
        
    ### Create Tag branches
    for var in variables["tag"]:
        df = df.Define("tag_"+var, "getVariables(TPPairs, Muon_"+var+", 1)")

    ### Additional variables
    df = df.Define("eventIdx", "RVecD(pair_mass.size(), event)")
        
    loop_time = time.time()
    print("Time taken to loop = ",(loop_time -start))
    
    # ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    '''
    #This gives you a  numpy.array(RVec)
    npy = df.AsNumpy(variables["save"])
    
    #This gives you a pd dataframe where each element is a Rvec. Not the format we want. 
    #Need to flatten it before using it form Tag and Probe 
    #df_pd = pd.DataFrame(npy)
    
    #print("Content of the ROOT.RDataFrame as pandas.DataFrame:\n{}\n".format(df_pd))
    #default_pd_time = time.time()
    #print("Time taken to save default pd dataframe = ",(default_pd_time - loop_time))
    
    #Flatten implementations. Below you can see 2 different implementations.
    
    #Using the flatten method of Awkward array. Slow compare to the numpy implementation
    #Numpy option is better so no need to use it

    test_df = {}
    for key in npy:
        test_ak = [ak.Array(v) for v in npy[key]]
        test_np = ak.to_numpy(ak.flatten(test_ak))
        test_df[key] = test_np
    df_ak = pd.DataFrame(test_df)
    
    df_ak.to_parquet("tnp_ak.parquet", engine='fastparquet')
    
    df_ak_time = time.time()

    print("Time taken to create and save df_ak = ", (df_ak_time - loop_time))
    '''
    # -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
    
    #######
    ####### Test chunck apporach with awkward 2.4!
    #######
        
    chunksize = 10_000
    nIterations = max(ceil(df.Count().GetValue() / chunksize), 1)
    if name_tag == "":
        outFile = samples[era]["output"]
    else:
        outFile = samples[era]["output"].split(".parquet")[0] + "_" + name_tag + ".parquet"
    branches = variables["save"]
    first = True

    default_pd_time = time.time()

    print("Total number of iterations -> " + str(nIterations))
    
    for i in range(nIterations):
        #if (i%10 == 0): print("Iteration: " + str(i))
        print("Iteration: " + str(i))
        _df = df.Range( i * chunksize, (i+1) * chunksize)
        events = ak.from_rdataframe(_df, branches)
        
        def getBranchFlatten(events, branch):
            ak_array = [ak.Array(v) for v in events[branch]]
            np_array = ak.to_numpy(ak.flatten(ak_array))
            return np_array
        
        df_np = {}
        for key in branches:
            df_np[key] = getBranchFlatten(events, key)
        df_ak = pd.DataFrame(df_np)
        if first:
            df_ak.to_parquet(outFile, engine='fastparquet', append=False)
            first=False
        else:
            df_ak.to_parquet(outFile, engine='fastparquet', append=True)

    print("Parquet done!")
    df_ak_time = time.time()  
    print("Time to create and save parquet = ", (df_ak_time - default_pd_time)) 

    # -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
    '''
    df_np_time = time.time()
    
    #Numpy implementation. Flatten option of numpy directly does not work since this is 
    #a jagged array. 
    test_df_np ={}
    for key in npy:
        test_list = [list(v) for v in npy[key]]
        flat_list = np.concatenate(test_list).flat
        test_df_np[key] = flat_list
    df_np = pd.DataFrame(test_df_np)
    print(df_np)
    
    print("Time taken to create df_np = ", (df_np_time - default_pd_time))

    outputName = f"test_{iteration_no}"
    
    df_np.to_parquet(outputPath+outputName+".parquet")
    
    df_save_time = time.time()
    
    print("Time taken to save df_ak = ", (df_save_time-df_np_time))
    
    print("Total time = ", (df_np_time - start))

    #out_file = uproot.recreate(samples[era]["root_file"])
    #out_file["tnp_tree"] = df_np
    '''

def build_condor_submit(era, dryRun=0):
    mypath = os.path.abspath(os.getcwd())
    
    fSh = ""
    with open(mypath + "/start.sh") as file:
        fSh += file.read()

    condorDir = mypath + "/condor"
    Path(condorDir).mkdir(parents=True, exist_ok=True)
    
    files = []
    cmd = "find {} -wholename '*/*.root'".format(samples[era]["input"])
    fnames = subprocess.check_output(cmd, shell=True).strip().split(b'\n')
    files = files + [fname.decode('ascii') for fname in fnames]

    fSub = """
universe = vanilla
executable = condor/$(Folder)/run.sh

should_transfer_files = YES
transfer_input_files = Muon_TnP_cfg.py,TNP_Muon_POG.h,TNP_Muon_POG.py,headers.hh

output = condor/$(Folder)/out.txt
error  = condor/$(Folder)/err.txt
log    = condor/$(Folder)/log.txt

request_cpus   = 1
request_memory = 12GB
request_disk   = 10GB
requirements = (OpSysAndVer =?= "AlmaLinux9")
+JobFlavour = "workday"

queue 1 Folder in ALLTAGS
"""

    allTags = []
    
    for ff in files:
        folder_tag = ff.split("/")[-1].split(".root")[0]
        jobDir = condorDir + "/" + folder_tag
        Path(jobDir).mkdir(parents=True, exist_ok=True)

        job_fSh = fSh
        job_fSh = job_fSh + "\n"
        job_fSh = job_fSh + "time python TNP_Muon_POG.py -p " + era + " -s 0 -F " + ff
        with open(jobDir + "/run.sh", "w") as file:
            file.write(job_fSh)

        os.system("chmod +x " + jobDir + "/run.sh")

        allTags.append(folder_tag)
    
    fSub = fSub.replace("ALLTAGS", " ".join(allTags))
    with open("condor_submit_"+ era +".jdl", "w") as file:
        file.write(fSub)

    if dryRun==1:
        print("Submit jobs doing: \n")
        print("condor_submit condor_submit_"+ era +".jdl")
    else:
        proc = subprocess.Popen(
            "condor_submit condor_submit_"+ era +".jdl", shell=True
        )
        proc.wait()
        
    
def main():
    parser = defaultParser()
    args = parser.parse_args()
    
    prodName = args.prod
    doSubmit = args.doSubmit
    selFile = args.selFile
    dryRun = args.dryRun
    
    if prodName not in samples:
        print("Production name not valid!!!! Check Muon_TnP_cfg.py file!")
        exit
        
    if doSubmit==0 and selFile=="":
        create_TnP_pairs(prodName)
    elif doSubmit==0 and selFile!="":
        create_TnP_pairs(prodName, selFile)
    elif doSubmit==1:
        build_condor_submit(prodName, dryRun)
        
        
    
if __name__ == '__main__':
    main()
    print("DONE!")

    
