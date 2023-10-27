#ifndef HEADERS_HH
#define HEADERS_HH

#include "ROOT/RDataFrame.hxx"
#include "ROOT/RVec.hxx"
#include "TCanvas.h"
#include "TH1D.h"
#include "TLatex.h"
#include "Math/Vector4D.h"
#include "TStyle.h"
#include <string>

using namespace ROOT;
using namespace ROOT::VecOps;


template<typename container>
float Alt(container c, int index, float alt){
  if (index < c.size()) {
    return c[index];
  }
  else{
    return alt;
  }
}

#endif
