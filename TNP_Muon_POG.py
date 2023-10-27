#!/usr/bin/env python


import ROOT
import numpy as np
import awkward as ak
from sys import exit
import sys
import pandas as pd
import time
from Muon_TnP_cfg import *

ROOT.gInterpreter.ProcessLine(".O3")
ROOT.ROOT.EnableImplicitMT()
ROOT.gInterpreter.Declare('#include "headers.hh"')
ROOT.gInterpreter.Declare('#include "TNP_Muon_POG.h"') 



def create_TnP_pairs(era):
    
    start = time.time()
    
    
    #### Read input files
    
    df = ROOT.RDataFrame("Events",samples[era]["input"])
    
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
        
        
    #This gives you a  numpy.array(RVec)
    npy = df.AsNumpy(variables["save"])
    
    loop_time = time.time()
    print("Time taken to loop = ",(loop_time -start))
    
    #This gives you a pd dataframe where each element is a Rvec. Not the format we want. 
    #Need to flatten it before using it form Tag and Probe 
    df_pd = pd.DataFrame(npy)
    
    print("Content of the ROOT.RDataFrame as pandas.DataFrame:\n{}\n".format(df_pd))
    default_pd_time = time.time()
    print("Time taken to save default pd dataframe = ",(default_pd_time - loop_time))
    
    #Flatten implementations. Below you can see 2 different implementations.
    
    #Using the flatten method of Awkward array. Slow compare to the numpy implementation
    test_df = {}
    for key in npy:
        test_ak = [ak.Array(v) for v in npy[key]]
        test_np = ak.to_numpy(ak.flatten(test_ak))
        test_df[key] = test_np
    df_ak = pd.DataFrame(test_df)
    print(df_ak)
    
    df_ak_time = time.time()
    print("Time taken to create df_ak = ", (df_ak_time - default_pd_time))
    
    #Numpy implementation. Flatten option of numpy directly does not work since this is 
    #a jagged array. 
    test_df_np ={}
    for key in npy:
        test_list = [list(v) for v in npy[key]]
        flat_list = np.concatenate(test_list).flat
        # flat_list = np.concatenate(test_list)
        test_df_np[key] = flat_list
    df_np = pd.DataFrame(test_df_np)
    print(df_np)
    
    df_np_time = time.time()
    print("Time taken to create df_np = ", (df_np_time - df_ak_time))
    
    df_ak.to_parquet(samples[era]["output"])
    
    df_save_time = time.time()
    
    print("Time taken to save df_ak = ", (df_save_time-df_np_time))
    
    print("Total time = ", (df_np_time - start))

    

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Introduce an era")
        exit
    era = sys.argv[1]
    create_TnP_pairs(era)
    print("DONE!")
