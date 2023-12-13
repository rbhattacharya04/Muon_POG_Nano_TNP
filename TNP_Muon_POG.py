#!/usr/bin/env python


import ROOT
from sys import exit
import time
from Muon_TnP_cfg import *
import os
import subprocess
import sys
sys.path.insert(0, list(filter(lambda k: 'myenv' in k, sys.path))[0])
import uproot
import awkward as ak
import pandas as pd
import numpy as np

from math import ceil

ROOT.gInterpreter.ProcessLine(".O3")
#ROOT.EnableImplicitMT(1)
ROOT.gInterpreter.Declare('#include "headers.hh"')
ROOT.gInterpreter.Declare('#include "TNP_Muon_POG.h"') 



def create_TnP_pairs(era):
    
    start = time.time()

    print("Start computation!")
    
    #### Read input files
    files = []
    cmd = "find {} -wholename '*/*.root'".format(samples[era]["input"])
    fnames = subprocess.check_output(cmd, shell=True).strip().split(b'\n')
    files = files + [fname.decode('ascii') for fname in fnames]

    print(files[0:10])

    files = files[0:10]
    #files = ["/eos/user/r/rbhattac/Muon_POG_NanoAOD_v2/Muon/NanoMuonPOGData2022F/231103_232820/0000/NanoMuonPOGData_22F_test_1-1.root"]
    filenames = ROOT.std.vector('string')()
    for name in files: filenames.push_back(name)

    print(filenames)

    print("Creating datframe")
    df = ROOT.RDataFrame("Events",filenames)

    print("Number of events -> " + str(df.Count().GetValue()))
    
    # just for utility
    df = df.Alias("Tag_pt",  "Muon_pt")
    df = df.Alias("Tag_eta", "Muon_eta")
    df = df.Alias("Tag_phi", "Muon_phi")
    df = df.Alias("Tag_charge", "Muon_charge")
    
    
    ### Match between TrigObj and Muon collections
    df = df.Define("Muon_trigIdx", "CreateTrigIndex(Muon_eta, Muon_phi, TrigObj_eta, TrigObj_phi, 0.1)")
    df = df.Define("Muon_isTrig", "Muon_trigIdx > 0")
    df = df.Define("Muon_HLTIsoMu24", "TrigObj_filterBits[Muon_trigIdx] & 8")
    
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
    
    ### Create probe branches
    for var in variables["probe"]:
        df = df.Define("probe_"+var, "getVariables(TPPairs, Muon_"+var+", 2)")
        
    ### Create Tag branches
    for var in variables["tag"]:
        df = df.Define("tag_"+var, "getVariables(TPPairs, Muon_"+var+", 1)")


    loop_time = time.time()
    print("Time taken to loop = ",(loop_time -start))
    
    # ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    
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

    # -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
    
    #######
    ####### Test chunck apporach with awkward 2.4!
    #######
        
    chunksize = 10_000
    nIterations = max(ceil(df.Count().GetValue() / chunksize), 1)
    outFile = "test_" + samples[era]["output"]
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
    
    df_np.to_parquet(samples[era]["output"])
    
    df_save_time = time.time()
    
    print("Time taken to save df_ak = ", (df_save_time-df_np_time))
    
    print("Total time = ", (df_np_time - start))

    #out_file = uproot.recreate(samples[era]["root_file"])
    #out_file["tnp_tree"] = df_np
    '''
    

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Introduce an era")
        exit
    era = sys.argv[1]
    create_TnP_pairs(era)
    print("DONE!")
