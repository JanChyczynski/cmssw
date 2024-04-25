#include "CondCore/CondDB/interface/ConnectionPool.h"
#include "CondCore/PopCon/interface/OnlinePopCon.h"
#include "FWCore/MessageLogger/interface/MessageLogger.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"

//#include <iostream>

namespace popcon {

  constexpr const char* const OnlinePopCon::s_version;

  OnlinePopCon::OnlinePopCon(const edm::ParameterSet& pset)
      : m_recordName(pset.getParameter<std::string>("record")),
        m_useLockRecors(pset.getUntrackedParameter<bool>("useLockRecords")) {
    edm::LogInfo("OnlinePopCon")
        << "This is OnlinePopCon (Populator of Condition) v" << s_version << ".\n"
        << "Please report any problem and feature request through the JIRA project CMSCONDDB.\n";
  }

  void OnlinePopCon::initialize() {
    // Check if DB service is available
    if (!m_dbService.isAvailable()) {
      throw Exception("OnlinePopCon", "[initialize] DBService not available");
    }

    // If requested, lock records
    if (m_useLockRecors) {
      m_dbService->lockRecords();
    }

    // Start DB logging service
    m_dbLoggerReturn_ = 0;
    m_dbService->logger().start();
    m_dbService->logger().logInfo() << "OnlinePopCon::initialize - begin logging for record: " << m_recordName;
  }

  void OnlinePopCon::finalize() {
    // Check if DB service is available
    if (!m_dbService.isAvailable()) {
      throw Exception("OnlinePopCon", "[finalize] DBService not available");
    }

    // Release locks if previously locked
    if (m_useLockRecors) {
      m_dbService->logger().logInfo() << "OnlinePopCon::finalize - releasing locks";
      m_dbService->releaseLocks();
    }

    // Stop DB logging service
    m_dbService->logger().logInfo() << "OnlinePopCon::finalize - end logging for record: " << m_recordName;
    m_dbService->logger().end(m_dbLoggerReturn_);
  }

}  // namespace popcon
