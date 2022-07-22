#include "FWCore/Framework/interface/Frameworkfwd.h"
#include "FWCore/Framework/interface/one/EDAnalyzer.h"
#include "FWCore/Framework/interface/Event.h"
#include "FWCore/Framework/interface/EventSetup.h"
#include "FWCore/Framework/interface/ESHandle.h"
#include "FWCore/Framework/interface/MakerMacros.h"

#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "FWCore/ServiceRegistry/interface/Service.h"

#include "CondCore/DBOutputService/interface/PoolDBOutputService.h"

#include "CondFormats/RunInfo/interface/LHCInfoPerFill.h"
#include "CondFormats/DataRecord/interface/LHCInfoPerFillRcd.h"

#include <memory>
#include <iostream>

class LHCInfoPerFillWriter : public edm::one::EDAnalyzer<> {
public:
  explicit LHCInfoPerFillWriter(const edm::ParameterSet&) {}

private:
  void beginJob() override {}
  void analyze(const edm::Event&, const edm::EventSetup&) override;
  void endJob() override {}
};

void LHCInfoPerFillWriter::analyze(const edm::Event& iEvent, const edm::EventSetup& iSetup) {
  LHCInfoPerFill lhcInfoPerFill;
  lhcInfoPerFill.setFillNumber(3);
  lhcInfoPerFill.setBunchesInBeam1(10);
  lhcInfoPerFill.setBunchesInBeam2(8);
  lhcInfoPerFill.setCollidingBunches(5);
  lhcInfoPerFill.setTargetBunches(4);
  lhcInfoPerFill.setFillType(lhcInfoPerFill.PROTONS);
  lhcInfoPerFill.setParticleTypeForBeam1(lhcInfoPerFill.PROTON);
  lhcInfoPerFill.setParticleTypeForBeam2(lhcInfoPerFill.PROTON);
  lhcInfoPerFill.setIntensityForBeam1(21.37);
  lhcInfoPerFill.setIntensityForBeam2(12.23);
  lhcInfoPerFill.setEnergy(1.12);
  lhcInfoPerFill.setDelivLumi(2.34);
  lhcInfoPerFill.setRecLumi(3.43);
  lhcInfoPerFill.setInstLumi(4.345);
  lhcInfoPerFill.setInstLumiError(5.567);
  lhcInfoPerFill.setCreationTime(65766);
  lhcInfoPerFill.setBeginTime(76788);
  lhcInfoPerFill.setEndTime(89090);
  lhcInfoPerFill.setInjectionScheme("None");
  lhcInfoPerFill.setLumiPerBX({3.4, 45.3});
  lhcInfoPerFill.setLhcState("some lhcState");
  lhcInfoPerFill.setLhcComment("some lhcComment");
  lhcInfoPerFill.setCtppsStatus("some ctppsStatus");
  lhcInfoPerFill.setBeam1VC({45.6, 67.4});
  lhcInfoPerFill.setBeam2VC({245.56, 56.45});
  lhcInfoPerFill.setBeam1RF({56.78, 34.3});
  lhcInfoPerFill.setBeam2RF({123.566, 5767});

  // store the info into a DB object
  edm::Service<cond::service::PoolDBOutputService> poolDbService;
  if (poolDbService.isAvailable())
    poolDbService->writeOneIOV(lhcInfoPerFill, poolDbService->currentTime(), "LHCInfoPerFillRcd");
  else
    throw cms::Exception("LHCInfoPerFillWriter") << "PoolDBService required.";
}

DEFINE_FWK_MODULE(LHCInfoPerFillWriter);