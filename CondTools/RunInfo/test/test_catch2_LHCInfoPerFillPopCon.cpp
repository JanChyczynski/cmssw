#define CATCH_CONFIG_MAIN

#include "catch.hpp"

#include "CondTools/RunInfo/interface/LHCInfoPerFillPopConSourceHandler.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"


TEST_CASE("LHCInfoPerFillPopConSourceHandler instantiation", "[LHCInfoPerFillPopConSourceHandler]") {
    edm::ParameterSet pset;
    // based on the default values in the src/CondTools/RunInfo/python/LHCInfoPerFillPopConAnalyzer_cfg.py add arguements
    pset.addUntrackedParameter<bool>("debug", false);
    pset.addUntrackedParameter<std::string>("startTime", "2023-01-01 00:00:00");
    pset.addUntrackedParameter<std::string>("endTime", "2023-12-31 23:59:59");
    pset.addUntrackedParameter<bool>("endFill", true);
    pset.addUntrackedParameter<std::string>("name", "LHCInfoPerFillPopConSourceHandler");
    pset.addUntrackedParameter<std::string>("connectionString", "oracle://cms_orcon_prod/CMS_CONDITIONS");
    pset.addUntrackedParameter<std::string>("ecalConnectionString", "oracle://cms_orcon_prod/CMS_CONDITIONS");
    pset.addUntrackedParameter<std::string>("authenticationPath", "/afs/cern.ch/cms/DB/conddb");
    pset.addUntrackedParameter<std::string>("omsBaseUrl", "https://oms.cern.ch/cern/oms");
    pset.addUntrackedParameter<double>("minEnergy", 450.0);
    pset.addUntrackedParameter<double>("maxEnergy", 8000.0); 
    LHCInfoPerFillPopConSourceHandler handler(pset);
    REQUIRE(true);
}