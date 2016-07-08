/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <rpc/dc.hpp>
#include <rpc/distributed_event_log.hpp>

namespace graphlab {
namespace warp {

DECLARE_EVENT(EVENT_WARP_MAPREDUCE_COUNT)
DECLARE_EVENT(EVENT_WARP_BROADCAST_COUNT)
DECLARE_EVENT(EVENT_WARP_TRANSFORM_COUNT)
DECLARE_EVENT(EVENT_WARP_PARFOR_VERTEX_COUNT)
DECLARE_EVENT(EVENT_WARP_ENGINE_SIGNAL)
DECLARE_EVENT(EVENT_WARP_ENGINE_UF_COUNT)
DECLARE_EVENT(EVENT_WARP_ENGINE_UF_TIME)

void initialize_counters() {
  static bool is_initialized = false;

  if (!is_initialized) {
    INITIALIZE_EVENT_LOG;
    graphlab::estimate_ticks_per_second();
    ADD_CUMULATIVE_EVENT(EVENT_WARP_MAPREDUCE_COUNT, "Warp::MapReduce", "Calls");
    ADD_CUMULATIVE_EVENT(EVENT_WARP_BROADCAST_COUNT, "Warp::Broadcast", "Calls");
    ADD_CUMULATIVE_EVENT(EVENT_WARP_TRANSFORM_COUNT, "Warp::Transform", "Calls");
    ADD_CUMULATIVE_EVENT(EVENT_WARP_PARFOR_VERTEX_COUNT, "Warp::Parfor", "Vertices");
    ADD_CUMULATIVE_EVENT(EVENT_WARP_ENGINE_SIGNAL, "Warp::Engine::Signal", "Calls");
    ADD_CUMULATIVE_EVENT(EVENT_WARP_ENGINE_UF_COUNT, "Warp::Engine::Update", "Calls");
    ADD_CUMULATIVE_EVENT(EVENT_WARP_ENGINE_UF_TIME, "Warp::Engine::UpdateTotalTime", "ms");
    ADD_AVERAGE_EVENT(EVENT_WARP_ENGINE_UF_TIME, EVENT_WARP_ENGINE_UF_COUNT,
                      "Warp::Engine::UpdateAverageTime", "ms");
    is_initialized = true;
  }
}

} // namespace warp

} // namespace graphlab
