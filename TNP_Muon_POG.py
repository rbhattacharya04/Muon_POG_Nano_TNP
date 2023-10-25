import ROOT
import numpy as np
import awkward as ak
from sys import exit
import pandas as pd
import time

ROOT.gInterpreter.ProcessLine(".O3")
ROOT.ROOT.EnableImplicitMT()
ROOT.gInterpreter.Declare('#include "TNP_Muon_POG.h"') 

start = time.time()

df = ROOT.RDataFrame("Events","NanoV9Data_MuonPOG_22F.root")

# just for utility
df = df.Alias("Tag_pt",  "Muon_pt")
df = df.Alias("Tag_eta", "Muon_eta")
df = df.Alias("Tag_phi", "Muon_phi")
df = df.Alias("Tag_charge", "Muon_charge")

df = df.Define("Tag_Muons", "Muon_pt > 26 && abs(Muon_eta) < 2.4 &&  Muon_tightId")

df = df.Define("Probe_Muons", "Muon_isTracker && Muon_pt > 24 && abs(Muon_eta) < 2.4")

df = df.Define("All_TPPairs", "CreateTPPair(Tag_Muons, Probe_Muons, 1, Tag_charge, Muon_charge, 1)")

df = df.Define("All_TPmass","getTPVariables(All_TPPairs, Tag_pt, Tag_eta, Tag_phi, Muon_pt, Muon_eta, Muon_phi, 4)")

massLow  =  60
massHigh = 120

massCut = f"All_TPmass > {massLow} && All_TPmass < {massHigh}"

df = df.Define("TPPairs", f"All_TPPairs[{massCut}]")
df = df.Filter("TPPairs.size() > 0")

df = df.Define("pair_mass",  f"All_TPmass[{massCut}]")
df = df.Define("pair_pt","getTPVariables(TPPairs, Tag_pt, Tag_eta, Tag_phi, Muon_pt, Muon_eta, Muon_phi, 1)")
df = df.Define("pair_eta","getTPVariables(TPPairs, Tag_pt, Tag_eta, Tag_phi, Muon_pt, Muon_eta, Muon_phi, 2)")
df = df.Define("pair_phi","getTPVariables(TPPairs, Tag_pt, Tag_eta, Tag_phi, Muon_pt, Muon_eta, Muon_phi, 3)")

df = df.Define("probe_charge", "getVariables(TPPairs, Muon_charge, 2)")
df = df.Define("probe_pt",     "getVariables(TPPairs, Muon_pt,     2)")
df = df.Define("probe_eta",    "getVariables(TPPairs, Muon_eta,    2)")
df = df.Define("probe_isGlobal",    "getVariables(TPPairs, Muon_isGlobal,    2)")


df = df.Define("tag_charge", "getVariables(TPPairs, Muon_charge, 1)")
df = df.Define("tag_pt",     "getVariables(TPPairs, Muon_pt,     1)")
df = df.Define("tag_eta",    "getVariables(TPPairs, Muon_eta,    1)")
df = df.Define("tag_isGlobal",    "getVariables(TPPairs, Muon_isGlobal,   1)")

variables = ["pair_mass","pair_pt","pair_eta","pair_phi","probe_charge","probe_pt","probe_eta","probe_isGlobal","tag_charge","tag_pt","tag_eta","tag_isGlobal"]

#This gives you a  numpy.array(RVec)
npy = df.AsNumpy(variables)

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

print("Total time = ", (df_np_time - start))
