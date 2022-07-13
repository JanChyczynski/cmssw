#include "CondFormats/RunInfo/interface/LHCInfoPerLS.h"
//#include "CondFormats/Common/interface/TimeConversions.h"
#include <algorithm>
#include <iterator>
#include <vector>
#include <stdexcept>

LHCInfoPerLS::LHCInfoPerLS() {
  m_intParams.resize(ISIZE, std::vector<unsigned int>(1, 0));
  m_floatParams.resize(FSIZE, std::vector<float>(1, 0.));
}

LHCInfoPerLS::LHCInfoPerLS(const LHCInfoPerLS& rhs)
    : m_intParams(rhs.m_intParams),
      m_floatParams(rhs.m_floatParams) {}

LHCInfoPerLS::~LHCInfoPerLS() {}

namespace LHCInfoPerLSImpl {
  template <typename T>
  const T& getParams(const std::vector<T>& params, size_t index) {
    if (index >= params.size())
      throw std::out_of_range("Parameter with index " + std::to_string(index) + " is out of range.");
    return params[index];
  }

  template <typename T>
  T& accessParams(std::vector<T>& params, size_t index) {
    if (index >= params.size())
      throw std::out_of_range("Parameter with index " + std::to_string(index) + " is out of range.");
    return params[index];
  }

  template <typename T>
  const T& getOneParam(const std::vector<std::vector<T> >& params, size_t index) {
    if (index >= params.size())
      throw std::out_of_range("Parameter with index " + std::to_string(index) + " is out of range.");
    const std::vector<T>& inner = params[index];
    if (inner.empty())
      throw std::out_of_range("Parameter with index " + std::to_string(index) + " type=" + typeid(T).name() +
                              " has no value stored.");
    return inner[0];
  }

  template <typename T>
  void setOneParam(std::vector<std::vector<T> >& params, size_t index, const T& value) {
    if (index >= params.size())
      throw std::out_of_range("Parameter with index " + std::to_string(index) + " is out of range.");
    params[index] = std::vector<T>(1, value);
  }

  template <typename T>
  void setParams(std::vector<T>& params, size_t index, const T& value) {
    if (index >= params.size())
      throw std::out_of_range("Parameter with index " + std::to_string(index) + " is out of range.");
    params[index] = value;
  }

}  // namespace LHCInfoPerLSImpl

//getters
float const LHCInfoPerLS::crossingAngle() const { return LHCInfoPerLSImpl::getOneParam(m_floatParams, CROSSING_ANGLE); }

float const LHCInfoPerLS::betaStar() const { return LHCInfoPerLSImpl::getOneParam(m_floatParams, BETA_STAR); }

unsigned int const& LHCInfoPerLS::lumiSection() const { return LHCInfoPerLSImpl::getOneParam(m_intParams, LUMI_SECTION); }

//setters
void LHCInfoPerLS::setCrossingAngle(float const& angle) { LHCInfoPerLSImpl::setOneParam(m_floatParams, CROSSING_ANGLE, angle); }

void LHCInfoPerLS::setBetaStar(float const& betaStar) { LHCInfoPerLSImpl::setOneParam(m_floatParams, BETA_STAR, betaStar); }

void LHCInfoPerLS::setLumiSection(unsigned int const& lumiSection) {
  LHCInfoPerLSImpl::setOneParam(m_intParams, LUMI_SECTION, lumiSection);
}

//sets all values in one go
void LHCInfoPerLS::setInfo(float const& angle,
                      float const& beta,
                      unsigned int const& lumiSection) {
  this->setCrossingAngle(angle);
  this->setBetaStar(beta);
  this->setLumiSection(lumiSection);
}

void LHCInfoPerLS::print(std::stringstream& ss) const {
  ss << "Crossing angle (urad): " << this->crossingAngle() << std::endl
     << "Beta star (cm): " << this->betaStar() << std::endl
     << "Lumi section: " << this->lumiSection() << std::endl;
}

std::ostream& operator<<(std::ostream& os, LHCInfoPerLS beamInfo) {
  std::stringstream ss;
  beamInfo.print(ss);
  os << ss.str();
  return os;
}

bool LHCInfoPerLS::equals(const LHCInfoPerLS& rhs) const {
  if (m_isData != rhs.m_isData)
    return false;
  if (m_intParams != rhs.m_intParams)
    return false;
  if (m_floatParams != rhs.m_floatParams)
    return false;
  return true;
}

bool LHCInfoPerLS::empty() const { return m_intParams[0].empty(); }
