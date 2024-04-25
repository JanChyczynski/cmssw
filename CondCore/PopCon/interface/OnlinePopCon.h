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
    void initialize();
    void finalize();

  private:
    edm::Service<cond::service::OnlineDBOutputService> m_dbService;  // DB service
    int m_dbLoggerReturn_;                                           // DB logger return value
    std::string m_recordName;                                        // output record name
    bool m_useLockRecors;                                            // whether to lock the records or not
    static constexpr const char* const s_version = "5.0";
  };

  template <typename Source>
  void OnlinePopCon::write(Source const& source) {
    // Initialize
    initialize();

    // Get data to be uploaded
    std::pair<Source::Container const*, std::string const> ret = source();  // FIXME: how to initialize the source?
    Source::Container const& iovs = *ret.first;

    // Check that only 1 iov/payload is provided
    if (iovs.size() > 1) {
      throw Exception("OnlinePopCon", "[write] More than one iov/payload provided!");
    }

    // If zero payloads to transfer, finalize and return
    if (iovs.empty()) {
      m_dbService->logger().logInfo() << "OnlinePopCon::write - Nothing to transfer";
      finalize();
      return;
    }

    // Upload
    // Check if DB service is available
    if (!m_dbService.isAvailable()) {
      throw Exception("OnlinePopCon", "[write] DBService not available");
    }

    // Get the only payload to transfer
    auto [since, payload] = *iovs.begin();

    // Log the original since
    m_dbService->logger().logInfo() << "OnlinePopCon::write - original since: " << since;

    // Upload the payload
    try {
      auto targetSince = m_dbService->writeIOVForNextLumisection(payload, m_recordName);
      m_dbService->logger().logInfo() << "OnlinePopCon::write - writeForNextLumisection successful!";
      m_dbService->logger().logInfo() << "OnlinePopCon::write - uploaded with since: " << targetSince;
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
