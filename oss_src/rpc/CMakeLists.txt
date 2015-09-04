project(rpc)

set(REQUIRED_LIBS logger libevent metric parallel random timer fiber network)
if(${HAS_MPI})
  LIST(APPEND REQUIRED_LIBS mpi)
endif()

make_library(rpc
  SOURCES
    dc_tcp_comm.cpp
    circular_char_buffer.cpp
    dc_stream_receive.cpp
    dc_buffered_stream_send2.cpp
    dc_global.cpp
    dc.cpp
    request_reply_handler.cpp
    dc_init_from_env.cpp
    dc_init_from_mpi.cpp
    dc_init_from_zookeeper.cpp
    async_consensus.cpp
    distributed_event_log.cpp
    delta_dht.cpp
    thread_local_send_buffer.cpp
    get_current_process_hash.cpp
    mpi_tools.cpp
    dc_registry.cpp
    dc_dist_object_lock.cpp
  REQUIRES
    ${REQUIRED_LIBS}
  EXTERNAL_VISIBILITY
)
