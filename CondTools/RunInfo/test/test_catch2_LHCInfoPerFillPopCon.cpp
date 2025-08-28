#define CATCH_CONFIG_MAIN

#include "catch.hpp"

#include "CondTools/RunInfo/interface/LHCInfoPerFillPopConSourceHandler.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"

struct LHCInfoPerFillPopConSourceHandlerProtectedAccessor : public LHCInfoPerFillPopConSourceHandler {
    using LHCInfoPerFillPopConSourceHandler::m_fillPayload;
    using LHCInfoPerFillPopConSourceHandler::addPayloadToBuffer;
    using LHCInfoPerFillPopConSourceHandler::m_tmpBuffer;
    using LHCInfoPerFillPopConSourceHandler::m_timestampToLumiid;

    LHCInfoPerFillPopConSourceHandlerProtectedAccessor(edm::ParameterSet const& pset)
        : LHCInfoPerFillPopConSourceHandler(pset) {}    
};

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
    //generate test for both endFill and duringFill modes 
    bool endFillMode = GENERATE(true, false);
    edm::ParameterSet pset = createDefaultPSet();
    pset.addUntrackedParameter<bool>("endFill", endFillMode);
    LHCInfoPerFillPopConSourceHandlerProtectedAccessor handler(pset);

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

TEST_CASE("LHCInfoPerFillPopConSourceHandler.addPayloadToBuffer works", "[addPayloadToBuffer]") {
    //generate test for both endFill and duringFill modes 
    bool endFillMode = GENERATE(true, false);
    edm::ParameterSet pset = createDefaultPSet();
    pset.addUntrackedParameter<bool>("endFill", endFillMode);
    LHCInfoPerFillPopConSourceHandlerProtectedAccessor handler(pset);

    // Create a mock OMSServiceResultRef
    boost::property_tree::ptree mockRow;

    mockRow.put("start_time", "2023-06-01 12:00:00");
    mockRow.put("delivered_lumi", 100.0f);
    mockRow.put("recorded_lumi", 80.0f);
    mockRow.put("run_number", "3500");
    mockRow.put("lumisection_number", "150");   
    cond::OMSServiceResultRef mockResultRef(&mockRow);

    // Set the m_fillPayload before calling addPayloadToBuffer
    handler.m_fillPayload = std::make_unique<LHCInfoPerFill>();
    handler.m_fillPayload->setFillNumber(1234);
    handler.m_fillPayload->setEnergy(6500.0);

    // Call the addPayloadToBuffer method
    handler.addPayloadToBuffer(mockResultRef);

    //verify that the payload was added to the tmpBuffer
    REQUIRE(handler.m_tmpBuffer.empty() == false);
    CHECK(handler.m_tmpBuffer.size() == 1);

    SECTION("addPayloadToBuffer adds correct payload to buffer") {
        auto& addedPayload = handler.m_tmpBuffer.front().second;
        CHECK(addedPayload->delivLumi() == 100.0f);
        CHECK(addedPayload->recLumi() == 80.0f);
        CHECK(addedPayload->fillNumber() == 1234);
        CHECK(addedPayload->energy() == 6500.0);
    }

    SECTION("addPayloadToBuffer adds correct IOV to buffer") {
        auto addedIov = handler.m_tmpBuffer.front().first;
        CHECK(addedIov == cond::time::from_boost(boost::posix_time::time_from_string("2023-06-01 12:00:00")));
    }

    if(!endFillMode) {
        SECTION("addPayloadToBuffer updates timestampToLumiid map in duringFill mode") {
            CAPTURE(endFillMode);
            REQUIRE(handler.m_timestampToLumiid.empty() == false);
            CHECK(handler.m_timestampToLumiid.size() == 1);
            auto it = handler.m_timestampToLumiid.begin();
            CHECK(it->first == cond::time::from_boost(boost::posix_time::time_from_string("2023-06-01 12:00:00")));
            CHECK(it->second == cond::time::lumiTime(3500, 150));
        }
    }
}