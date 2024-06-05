#include "CondCore/PopCon/interface/PopConAnalyzer.h"
#include "CondTools/RunInfo/interface/LHCInfoPerLSPopConSourceHandler.h"
#include "FWCore/Framework/interface/MakerMacros.h"

using LHCInfoPerLSPopConAnalyzer = popcon::PopConAnalyzer<LHCInfoPerLSPopConSourceHandler>;

DEFINE_FWK_MODULE(LHCInfoPerLSPopConAnalyzer);
