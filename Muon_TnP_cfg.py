### Very first implementation of Muon TnP configurtion file
variables = {
    'probe' : ["charge", "pt", "eta", "dxy", "dz", "isTracker", "isStandalone", "isGlobal", "HLTIsoMu24", "tightId", "mediumId", "looseId", "highPtId", "pfIsoId", "pfAbsIso04_all"],
    "tag"   : ["charge", "pt", "eta", "dxy", "dz", "isTracker", "isStandalone", "isGlobal", "HLTIsoMu24", "tightId", "mediumId", "looseId", "highPtId", "pfIsoId", "pfAbsIso04_all"],
    "save"  : ["pair_mass","pair_pt","pair_eta","pair_phi","event",
               "probe_charge","probe_pt","probe_eta","probe_isGlobal","probe_HLTIsoMu24","probe_dxy","probe_dz","probe_isTracker","probe_isStandalone","probe_tightId","probe_mediumId","probe_looseId","probe_highPtId","probe_pfIsoId","probe_pfAbsIso04_all",
               "tag_charge","tag_pt","tag_eta","tag_isGlobal","tag_HLTIsoMu24","tag_dxy","tag_dz","tag_isTracker","tag_isStandalone","tag_tightId","tag_mediumId","tag_looseId","tag_highPtId","tag_pfIsoId","tag_pfAbsIso04_all",
               ],
}

selection = {
    "probe" : "Muon_isStandalone && Muon_pt > 2 && abs(Muon_eta) < 2.4",
    "tag"   : "Muon_pt > 15 && abs(Muon_eta) < 2.4 &&  Muon_tightId && Muon_isTrig",
    "pair"  : "All_TPmass > 60 && All_TPmass < 120" 
}

samples = {
    "Run2022" : {
        'input'  : "/eos/user/r/rbhattac/Muon_POG_NanoAOD_v2/Muon/NanoMuonPOGData2022F/231103_232820/0000/",
        "output" : "/eos/cms/store/group/phys_muon//sblancof/nanoAOD/tnp.parquet",
        "lumiMask": "https://cms-service-dqmdc.web.cern.ch/CAF/certification/Collisions23/Cert_Collisions2023_366442_370790_Golden.json",
        "doSplit": True,
        "root_file": "tnp.root"
    }
}
