/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#include <boost/unordered_map.hpp>
#include <parallel/pthread_tools.hpp>
#include <metric/simple_metrics_service.hpp>
#include <metric/metrics_server.hpp>

namespace graphlab {

static mutex simple_metrics_lock;
typedef boost::unordered_map<std::string, std::vector<std::pair<double, double> > > simple_metrics_container_type;
typedef boost::unordered_map<std::string, std::pair<std::string, std::string> > simple_metrics_axis_type;

static simple_metrics_container_type simple_metrics_values;
static simple_metrics_axis_type simple_metrics_axis;

/**
 * simple metrics callback
 */
std::pair<std::string, std::string> 
simple_metrics_callback(std::map<std::string, std::string>& varmap) {
  std::stringstream strm;

  simple_metrics_lock.lock();
  // make a top level array
  strm << "[\n";
  simple_metrics_container_type::const_iterator iter = simple_metrics_values.begin();
  while(iter != simple_metrics_values.end()) {
    std::string xlab = "x";
    std::string ylab = "y";
    if (simple_metrics_axis.count(iter->first)) {
      xlab =  simple_metrics_axis[iter->first].first;
      ylab =  simple_metrics_axis[iter->first].second;
    }
    strm << "    {\n"
         << "      \"id\":\"" << iter->first << "\",\n"
         << "      \"name\": \"" << iter->first << "\",\n"
         << "      \"xlab\": \"" << xlab << "\",\n"
         << "      \"ylab\": \"" << ylab << "\",\n"
         << "      \"record\": [";

    for (size_t i = 0 ;i < iter->second.size(); ++i) {
      strm << " [" 
           << iter->second[i].first << ", " 
           << iter->second[i].second 
           << "] ";
      // add a comma if this is not the last entry
      if (i < iter->second.size() - 1) strm << ", ";
    }
    strm << "]\n"
         << "    }\n";
    ++iter;
    if (iter != simple_metrics_values.end()) strm << ",\n";
  }
  strm << "]\n";
  simple_metrics_lock.unlock();
  return std::make_pair(std::string("text/plain"), strm.str());
}


void add_simple_metric(std::string key, std::pair<double, double> value) {
  simple_metrics_lock.lock();
  simple_metrics_values[key].push_back(value);
  simple_metrics_lock.unlock();
}

void add_simple_metric_axis(std::string key, std::pair<std::string, std::string> xylab) {
  simple_metrics_lock.lock();
  simple_metrics_axis[key] = xylab;
  simple_metrics_lock.unlock();
}

void remove_simple_metric(std::string key) {
  simple_metrics_lock.lock();
  simple_metrics_values.erase(key);
  simple_metrics_axis.erase(key);
  simple_metrics_lock.unlock();
}

void clear_simple_metrics() {
  simple_metrics_lock.lock();
  simple_metrics_values.clear();
  simple_metrics_axis.clear();
  simple_metrics_lock.unlock();
}


} // namespace graphlab
