/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_UI_SIMPLE_METRICS_SERVICE_HPP
#define GRAPHLAB_UI_SIMPLE_METRICS_SERVICE_HPP
#include <string>
#include <utility>
namespace graphlab {

/**
 * \ingroup httpserver
 * Part of the simple_metrics service; adds a new entry to an existing axis. 
 *
 * \param key Graph Name to add to 
 * \param value value Datapoint to add to graph
 *
 * For a previous axis registered by add_simple_metric_axis, this 
 * appends a new (x,y) point to the axis.
 *
 * Example:
 * \code
 * // registers a graph with name "speed" and with X axis "time(h)" 
 * // and Y axis "mph"
 * add_simple_metric_axis("speed", {"time(h)","mph"});
 *
 * // registers a graph with name "distance_travelled" and with 
 * // X axis "time" and Y axis "miles"
 * add_simple_metric_axis("distance_travelled", {"time(h)","miles"});
 *
 * for (int t = 0; t < 10; ++t) {
 *   // graph denotes constant speed of 1 mph over 10 hours
 *   add_simple_metric("speed", {t, 1});
 *   // graph denotes distance travelled at 1mph (i.e. travelled t miles in t hours)
 *   add_simple_metric("distance_travelled", {t, t});
 * }
 * \endcode
 *
 * \see add_simple_metric_axis, remove_simple_metric, clear_simple_metrics
 */
void add_simple_metric(std::string key, std::pair<double, double> value); 

/**
 * \ingroup httpserver
 * Part of the simple_metrics service; registers a new graph with name "key" 
 * and with x-y axis titles in xylab.
 *
 * \param key New graph name
 * \param xylab A pair of x and y axis labels
 *
 * See graphlab::add_simple_metric for example of usage.
 *
 * \see add_simple_metric, remove_simple_metric, clear_simple_metrics
 */
void add_simple_metric_axis(std::string key, std::pair<std::string, std::string> xylab); 

/**
 * \ingroup httpserver
 * Part of the simple_metrics service; removes a graph with the name key
 *
 * \param key The name of the graph to remove
 *
 * \see add_simple_metric, add_simple_metric_axis, clear_simple_metrics
 */
void remove_simple_metric(std::string key); 

/**
 * \ingroup httpserver
 * Part of the simple_metrics service; removes all graphs.
 *
 * \see add_simple_metric, add_simple_metric_axis, remove_simple_metric
 */
void clear_simple_metrics(); 

} // namespace graphlab

#endif
