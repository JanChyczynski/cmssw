#define CATCH_CONFIG_MAIN

#include "catch.hpp"

#include "CondTools/RunInfo/interface/LHCInfoPerFillPopConSourceHandler.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"

// Helper function to create a default ParameterSet
edm::ParameterSet createDefaultPSet() {
    edm::ParameterSet pset;
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
    return pset;
}

TEST_CASE("LHCInfoPerFillPopConSourceHandler.isPayloadValid works", "[isPayloadValid]") {
    edm::ParameterSet pset = createDefaultPSet();
    LHCInfoPerFillPopConSourceHandler handler(pset);

    LHCInfoPerFill payload;

    SECTION("Energy within range is valid") {
        payload.setEnergy(6500.0);
        CHECK(handler.isPayloadValid(payload) == true);
    }

    SECTION("Energy at lower bound is valid") {
        payload.setEnergy(450.0);
        CHECK(handler.isPayloadValid(payload) == true);
    }

    SECTION("Energy at upper bound is valid") {
        payload.setEnergy(8000.0);
        CHECK(handler.isPayloadValid(payload) == true);
    }

    SECTION("Energy below range is invalid") {
        payload.setEnergy(400.0);
        CHECK(handler.isPayloadValid(payload) == false);
    }

    SECTION("Energy above range is invalid") {
        payload.setEnergy(8500.0);
        CHECK(handler.isPayloadValid(payload) == false);
    }

}