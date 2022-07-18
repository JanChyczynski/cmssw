#ifndef CondFormats_RunInfo_LHCInfoVectorizedFields_H
#define CondFormats_RunInfo_LHCInfoVectorizedFields_H

#include "CondFormats/Serialization/interface/Serializable.h"
#include <vector>

class LHCInfoVectorizedFields {
public:
  template <typename T>
  static const T& getParams(const std::vector<T>& params, size_t index);

  template <typename T>
  static T& accessParams(std::vector<T>& params, size_t index);

  template <typename T>
  static const T& getOneParam(const std::vector<std::vector<T> >& params, size_t index);

  template <typename T>
  static void setOneParam(std::vector<std::vector<T> >& params, size_t index, const T& value);

  template <typename T>
  static void setParams(std::vector<T>& params, size_t index, const T& value);

  COND_SERIALIZABLE;
};

#endif  // CondFormats_RunInfo_LHCInfoVectorizedFields_H