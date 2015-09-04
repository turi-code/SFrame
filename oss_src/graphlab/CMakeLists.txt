project(GraphLab)

subdirs(
  engine
  options
  scheduler
  util
  dht
  )




#/**************************************************************************/
#/*                                                                        */
#/*                           GraphLab Libraries                           */
#/*                                                                        */
#/**************************************************************************/

make_library(graphlab 
  SOURCES
    scheduler/scheduler_list.cpp
    scheduler/fifo_scheduler.cpp
    scheduler/priority_scheduler.cpp
    scheduler/sweep_scheduler.cpp
    scheduler/queued_fifo_scheduler.cpp
    options/command_line_options.cpp
    options/options_map.cpp
    util/safe_circular_char_buffer.cpp
    util/fs_util.cpp
    util/web_util.cpp
    util/inplace_lf_queue.cpp
    engine/warp_event_log.cpp
    singleton_destruction_ordering.cpp
  REQUIRES
    rpc logger fileio metric parallel random timer perf fiber util
)
