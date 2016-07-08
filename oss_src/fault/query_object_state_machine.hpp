/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef FAULT_QUERY_OBJECT_STATE_MACHINE_HPP
#define FAULT_QUERY_OBJECT_STATE_MACHINE_HPP
namespace libfault {

/*
 This is the state machine describing the states of a running query object.

 Masters are simple.
 masters are created in the MASTER_ACTIVE state
 MASTER_ACTIVE  --- (on deactivation)  --->  MASTER_INACTIVE


 Replicas are somewhat messier.
 Replicas are created in the REPLICA_INACTIVE state.
 And it sends a message to the current master to ask for a snapshot.

 REPLICA_INACTIVE -- (on receive snapshot) --> REPLICA_ACTIVE

 At any point if

 */
enum object_state {
  MASTER_INACTIVE,
  MASTER_ACTIVE

};

} // fault
#endif
