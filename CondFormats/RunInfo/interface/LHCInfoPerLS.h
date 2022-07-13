#ifndef CondFormats_RunInfo_LHCInfoPerLS_H
#define CondFormats_RunInfo_LHCInfoPerLS_H

#include "CondFormats/Serialization/interface/Serializable.h"
#include <bitset>
#include <iostream>
#include <string>
#include <sstream>
#include <vector>

class LHCInfoPerLS {
public:
  enum IntParamIndex {
    LUMI_SECTION = 0,
    ISIZE = 1
  };
  enum FloatParamIndex {
    CROSSING_ANGLE_X = 0,
    CROSSING_ANGLE_Y = 1,
    BETA_STAR_X = 2,
    BETA_STAR_Y = 3,
    FSIZE = 4
  };

  LHCInfoPerLS();
  LHCInfoPerLS(const LHCInfoPerLS& rhs);
  ~LHCInfoPerLS();

  //getters
  float const crossingAngleX() const;

  float const crossingAngleY() const;

  float const betaStarX() const;

  float const betaStarY() const;

  unsigned int const& lumiSection() const;

  //setters
  void setCrossingAngleX(float const& angle);

  void setCrossingAngleY(float const& angle);

  void setBetaStarX(float const& betaStar);

  void setBetaStarY(float const& betaStar);

  void setLumiSection(unsigned int const& lumiSection);

  //sets all values in one go
  void setInfo(float const& angleX,
               float const& angleY,
               float const& betaX,
               float const& betaY,
               unsigned int const& lumiSection);

  bool equals(const LHCInfoPerLS& rhs) const;

  bool empty() const;

  //dumping values on output stream
  void print(std::stringstream& ss) const;

private:
  bool m_isData = false;
  std::vector<std::vector<unsigned int> > m_intParams;
  std::vector<std::vector<float> > m_floatParams;

  COND_SERIALIZABLE;
};

std::ostream& operator<<(std::ostream&, LHCInfoPerLS lhcInfo);

#endif  // CondFormats_RunInfo_LHCInfoPerLS_H
