#include "ROOT/RDataFrame.hxx"
#include "ROOT/RVec.hxx"
#include "TCanvas.h"
#include "TH1D.h"
#include "TLatex.h"
#include "Math/Vector4D.h"
#include "TStyle.h"
#include <string>
#include "headers.hh"

using namespace ROOT;
using namespace ROOT::VecOps;

RVecI CreateTrigIndex(const RVecF Muon_eta,
		      const RVecF Muon_phi,
		      const RVecF TrigObj_eta,
		      const RVecF TrigObj_phi,
		      const float minDR)
{
  RVecI Muon_TrigIdx(Muon_eta.size(), -1);
  float dR = 999.9;
  for (int iMu=0; iMu < Muon_eta.size(); iMu++){
    float tmpDR = minDR;
    for (int iTr=0; iTr < TrigObj_eta.size(); iTr++){
      dR = DeltaR(Muon_eta[iMu], TrigObj_eta[iTr], Muon_phi[iMu], TrigObj_phi[iTr]);
      if (dR<tmpDR){
	tmpDR = dR;
	Muon_TrigIdx[iMu] = iTr;
      }
    }
  }
  return Muon_TrigIdx;
}

RVec<std::pair<int,int>> CreateTPPair(const RVec<Int_t> &Tag_muons,
                                      const RVec<Int_t> &Probe_Candidates,
                                      const int doOppositeCharge,
                                      const RVec<Int_t> &Tag_Charge,
                                      const RVec<Int_t> &Probe_charge,
                                      const int isSameCollection = true)
{
  RVec<std::pair<int,int>> TP_pairs;
  for (int iLep1 = 0; iLep1 < Tag_muons.size(); iLep1++) {
    if (not Tag_muons[iLep1]) continue;
    for(int iLep2 = 0; iLep2 < Probe_Candidates.size(); iLep2++){
      if (isSameCollection && (iLep1 == iLep2)) continue;
      if (not Probe_Candidates[iLep2]) continue;
      if (doOppositeCharge and (Tag_Charge[iLep1] == Probe_charge[iLep2])) continue;
      std::pair<int,int> TP_pair = std::make_pair(iLep1, iLep2);
      TP_pairs.push_back(TP_pair);
    }          
  }              
  return TP_pairs;
}

RVec<Float_t> getTPVariables(RVec<std::pair<int,int>> TPPairs,
                        RVec<Float_t> &Muon_pt, RVec<Float_t> &Muon_eta, RVec<Float_t> &Muon_phi,
                        RVec<Float_t> &Cand_pt, RVec<Float_t> &Cand_eta, RVec<Float_t> &Cand_phi,
                        int option = 4 /* 1 = pt, 2 = eta, 3 = phi, 4 = mass*/ )
{
  RVec<Float_t> TPVariables;
  for (int i=0;i<TPPairs.size();i++){
    std::pair<int,int> TPPair = TPPairs.at(i);
    int tag_index = TPPair.first;
    int probe_index = TPPair.second;
    ROOT::Math::PtEtaPhiMVector tag(  Muon_pt[tag_index],   Muon_eta[tag_index],   Muon_phi[tag_index],   0.106); /* Muon mass is used*/
    ROOT::Math::PtEtaPhiMVector probe(Cand_pt[probe_index], Cand_eta[probe_index], Cand_phi[probe_index], 0.106); /* Muon mass is used*/
    if (option == 1) TPVariables.push_back( (tag + probe).pt() );
    else if (option == 2 ) TPVariables.push_back( (tag + probe).eta() );
    else if (option == 3) TPVariables.push_back( (tag + probe).phi() );
    else if (option == 4) TPVariables.push_back( (tag + probe).mass() );
  }
  return TPVariables;
  
}


template <typename T>
RVec<T> getVariables(RVec<std::pair<int,int>> TPPairs,
                     RVec<T>  &Cand_variable,
                     int option /*1 for tag and 2 for probe*/)
{
    RVec<T>  Variables(TPPairs.size(), 0);
    for (int i = 0; i < TPPairs.size(); i++){
        std::pair<int, int> TPPair = TPPairs.at(i);
        T variable;
        if (option==1)      variable = Cand_variable.at(TPPair.first);
        else if (option==2) variable = Cand_variable.at(TPPair.second);
        Variables[i] = variable;
    }
    return Variables;
}
