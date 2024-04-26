#ifndef CONDCORE_POPCON_ONLINEPOPCON_H
#define CONDCORE_POPCON_ONLINEPOPCON_H

//
// Authors:
//  - Francesco Brivio (Milano-Bicocca)
//  - Jan Chyczynski   (AGH University of Krakow)
//

#include "CondCore/DBOutputService/interface/OnlineDBOutputService.h"
#include "CondCore/CondDB/interface/Time.h"
#include "CondCore/PopCon/interface/Exception.h"
#include "FWCore/MessageLogger/interface/MessageLogger.h"
#include "FWCore/ParameterSet/interface/ParameterSetfwd.h"
#include "FWCore/ServiceRegistry/interface/Service.h"

#include <algorithm>
#include <functional>
#include <iostream>
#include <string>
#include <vector>

namespace popcon {

  //  Populator of the Condition DB.
  //  Specific implementation for online lumi-based conditions (OnlineDBOutputService)

  class OnlinePopCon {
  public:
    typedef cond::Time_t Time_t;

    OnlinePopCon(const edm::ParameterSet& pset);

    ~OnlinePopCon() = default;

    template <typename Source>
    void write(Source const& source);

  private:
    cond::persistency::Session initialize();
    void finalize();

  private:
    edm::Service<cond::service::OnlineDBOutputService> m_dbService;  // DB service
    int m_dbLoggerReturn_;                                           // DB logger return value
    std::string m_recordName;                                        // output record name
    bool m_useLockRecors;                                            // whether to lock the records or not

    cond::TagInfo_t m_tagInfo;
    cond::LogDBEntry_t m_logDBEntry;


    static constexpr const char* const s_version = "5.0";
  };

  template <typename Source>
  void OnlinePopCon::write(Source const& source) {
    typedef typename Source::Container Container;

    // Get data to be uploaded
    std::pair<Container const*, std::string const> ret = source(initialize(), m_tagInfo, m_logDBEntry); // FIXME: how to initialize the source?
    Container const& iovs = *ret.first;

    // Check that only 1 iov/payload is provided
    if (iovs.size() > 1) {
      throw Exception("OnlinePopCon", "More than one iov/payload provided!");
    }
    if (iovs.empty()) {
      m_dbService->logger().logInfo() << "Nothing to transfer";
      finalize();
      return;
    }

    // Upload
    if (!m_dbService.isAvailable()) {
      throw Exception("OnlinePopCon", "m_dbService not available!");
    }
    
    auto payload = iovs.begin()->second;
    
    try {
      auto since = m_dbService->writeIOVForNextLumisection(payload, m_recordName);  //FIXME: here I should pass directly the payload as first argument
      m_dbService->logger().logInfo() << "OnlinePopCon::write - writeForNextLumisection successful!";
      m_dbService->logger().logInfo() << "Since " << since;
    } catch (const std::exception& e) {
      m_dbService->logger().logError() << "OnlinePopCon::write - Error writing record: " << m_recordName;
      m_dbService->logger().logError() << "Error is: " << e.what();
      m_dbLoggerReturn_ = 2;
    }

    // Finalize
    finalize();
  }

}  // namespace popcon

#endif  // CONDCORE_POPCON_ONLINEPOPCON_H
