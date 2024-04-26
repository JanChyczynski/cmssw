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

  cond::persistency::Session PopCon::prepareSession() {
    const std::string& connectionStr = m_dbService->session().connectionString();
    if (m_targetConnectionString.empty()) {
      m_targetSession = m_dbService->session();
      m_dbService->startTransaction();
    } else {
      cond::persistency::ConnectionPool connPool;
      connPool.setAuthenticationPath(m_authPath);
      connPool.setAuthenticationSystem(m_authSys);
      connPool.configure();
      m_targetSession = connPool.createSession(m_targetConnectionString);
      m_targetSession.transaction().start();
    }
    if (m_targetSession.existsDatabase() && m_targetSession.existsIov(m_tag)) {
      cond::persistency::IOVProxy iov = m_targetSession.readIov(m_tag);
      m_tagInfo.size = iov.sequenceSize();
      if (m_tagInfo.size > 0) {
        m_tagInfo.lastInterval = iov.getLast();
      }
      edm::LogInfo("PopCon") << "destination DB: " << connectionStr << ", target DB: "
                             << (m_targetConnectionString.empty() ? connectionStr : m_targetConnectionString) << "\n"
                             << "TAG: " << m_tag << ", last since/till: " << m_tagInfo.lastInterval.since << "/"
                             << m_tagInfo.lastInterval.till << ", size: " << m_tagInfo.size << "\n"
                             << std::endl;
    } else {
      edm::LogInfo("PopCon") << "destination DB: " << connectionStr << ", target DB: "
                             << (m_targetConnectionString.empty() ? connectionStr : m_targetConnectionString) << "\n"
                             << "TAG: " << m_tag << "; First writer to this new tag." << std::endl;
    }
    return m_targetSession;
  }

  void OnlinePopCon::initialize() {
    // Check if DB service is available
    if (!m_dbService.isAvailable()) {
      throw Exception("OnlinePopCon", "DBService not available");
    }

    m_dbService->forceInit(); // TODO: should we run this?

    m_tag = m_dbService->tag(m_record);
    m_tagInfo.name = m_tag;
    auto session = prepareSession()

    // If requested, lock records
    if (m_useLockRecors) {
      m_dbService->lockRecords();
    }

    // Start DB logging service
    m_dbLoggerReturn_ = 0;
    m_dbService->logger().start();
    m_dbService->logger().logInfo() << "OnlinePopCon::initialize - begin logging for record: " << m_recordName;
    return session;
  }

  void OnlinePopCon::finalize() {
    // If DB service available and records are locked, unlock them
    if (m_dbService.isAvailable()) {
      // Release locks
      if (m_useLockRecors) {
        m_dbService->logger().logInfo() << "OnlinePopCon::finalize - releasing locks";
        m_dbService->releaseLocks();
      }

      // Stop DB logging service
      m_dbService->logger().logInfo() << "OnlinePopCon::finalize - end logging for record: " << m_recordName;
      m_dbService->logger().end(m_dbLoggerReturn_);
    }
  }

}  // namespace popcon
