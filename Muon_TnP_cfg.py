### Very first implementation of Muon TnP configurtion file
variables = {
    'probe' : ["charge", "pt", "eta", "isGlobal", "HLTIsoMu24"],
    "tag"   : ["charge", "pt", "eta", "isGlobal", "HLTIsoMu24"],
    "save"  : ["pair_mass","pair_pt","pair_eta","pair_phi","probe_charge","probe_pt","probe_eta","probe_isGlobal","tag_charge","tag_pt","tag_eta","tag_isGlobal"],
}

selection = {
    "probe" : "Muon_isStandalone && Muon_pt > 2 && abs(Muon_eta) < 2.4",
    "tag"   : "Muon_pt > 15 && abs(Muon_eta) < 2.4 &&  Muon_tightId && Muon_isTrig",
    "pair"  : "All_TPmass > 60 && All_TPmass < 120" 
}

samples = {
    "Run2022" : {
        'input'  : ["NanoV9Data_MuonPOG_22F.root"],
        "output" : "tnp.parquet"
    }
}
