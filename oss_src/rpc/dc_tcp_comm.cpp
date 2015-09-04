/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
/**
 * Copyright (c) 2009 Carnegie Mellon UniversityIP addresses.
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://www.graphlab.ml.cmu.edu
 *
 */

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <netinet/tcp.h>
#include <ifaddrs.h>
#include <poll.h>

#include <limits>
#include <vector>
#include <string>
#include <map>

#include <boost/lexical_cast.hpp>
#include <boost/bind.hpp>
#include <logger/logger.hpp>
#include <rpc/dc_tcp_comm.hpp>
#include <rpc/dc_internal_types.hpp>
#include <rpc/get_current_process_hash.hpp>
#define compile_barrier() asm volatile("": : :"memory")

#include <graphlab/macros_def.hpp>

// prefix mangling if not Mac
#ifndef __APPLE__
#include <rpc/evwrapdef.h>
#endif
#include <event2/event.h>
#include <event2/thread.h>


// #define COMM_DEBUG
namespace graphlab {

static mutex libevent_init_lock;
static mutex comm_init_lock;

  namespace dc_impl {

    void dc_tcp_comm::init(const std::vector<std::string> &machines,
                           const std::map<std::string,std::string> &initopts,
                           procid_t curmachineid,
                           std::vector<dc_receive*> receiver_,
                           std::vector<dc_send*> sender_) {

      curid = curmachineid;
      ASSERT_LT(machines.size(), std::numeric_limits<procid_t>::max());
      nprocs = (procid_t)(machines.size());
      receiver = receiver_;
      sender = sender_;

      // insert machines into the address map
      all_addrs.resize(nprocs);
      portnums.resize(nprocs);
      assert(triggered_timeouts.size() >= nprocs);
      triggered_timeouts.clear();
      // fill all the socks
      sock.resize(nprocs);
      for (size_t i = 0;i < nprocs; ++i) {
        sock[i].id = i;
        sock[i].owner = this;
        sock[i].outsock = -1;
        sock[i].insock = -1;
        sock[i].inevent = NULL;
        sock[i].outevent = NULL;
        sock[i].wouldblock = false;
        sock[i].data.msg_name = NULL;
        sock[i].data.msg_namelen = 0;
        sock[i].data.msg_control = NULL;
        sock[i].data.msg_controllen = 0;
        sock[i].data.msg_flags = 0;
        sock[i].data.msg_iovlen = 0;
        sock[i].data.msg_iov = NULL;
      }

      program_md5 = get_current_process_hash();
      ASSERT_EQ(program_md5.length(), 32);
      comm_init_lock.lock();
      // parse the machines list, and extract the relevant address information
      for (size_t i = 0;i < machines.size(); ++i) {
        // extract the port number
        size_t pos = machines[i].find(":");
        ASSERT_NE(pos, std::string::npos);
        std::string address = machines[i].substr(0, pos);
        size_t port = boost::lexical_cast<size_t>(machines[i].substr(pos+1));

        struct hostent* ent = gethostbyname(address.c_str());
        ASSERT_EQ(ent->h_length, 4);
        uint32_t addr = *reinterpret_cast<uint32_t*>(ent->h_addr_list[0]);

        all_addrs[i] = addr;
        ASSERT_LT(port, 65536);
        portnums[i] = (uint16_t)(port);
      }
      comm_init_lock.unlock();
      network_bytessent = 0;
      buffered_len = 0;
      // if sock handle is set
      std::map<std::string, std::string>::const_iterator iter =
        initopts.find("__sockhandle__");
      if (iter != initopts.end()) {
        open_listening(atoi(iter->second.c_str()));
      } else {
        open_listening();
      }
      // to improve the "synchronous" nature of the connection setup,
      // the last machine will do this in reverse order.
      // To wait for all machines to connect to it, before it
      // tries to connect to all machines.
      // The last machine therefore essentially acts as the "barrier" leader
      if (curid != nprocs - 1) {
        // not the last machine.
        // Connect to everyone, EXCEPT the last machine
        // and wait for all incoming connections
        for(procid_t i = 0;i < nprocs - 1; ++i) connect(i);

        // wait for p - 1 incoming connections
        insock_lock.lock();
        while(1) {
          if (num_in_connected() == sock.size() - 1) break;
          insock_cond.wait(insock_lock);
        }
        insock_lock.unlock();

        // connect to the last machine
        connect(nprocs - 1);

        // wait for the last connection
        insock_lock.lock();
        while(1) {
          if (num_in_connected() == sock.size()) break;
          insock_cond.wait(insock_lock);
        }
        insock_lock.unlock();
      } else {
        // I am the last machine
        ASSERT_EQ(curid, nprocs - 1);
        // wait for all incoming connections before connecting to everyone
        // connect to myself
        connect(nprocs - 1);
        insock_lock.lock();
        while(1) {
          if (num_in_connected() == sock.size()) break;
          insock_cond.wait(insock_lock);
        }
        insock_lock.unlock();
        // now, when I know that machines 0 to #procs - 2 have
        // all established a connection to each other
        // connect to everyone. This is essentially equivalent to the
        // barrier release message
        for(size_t i = 0;i < nprocs; ++i) connect(i);
      }
      // everyone is connected.
      // Construct the eventbase
      construct_events();
      // we reserve the last 2 cores for communication
      inthreads.launch(boost::bind(&dc_tcp_comm::receive_loop, this, inevbase), thread::cpu_count() - 2);
      outthreads.launch(boost::bind(&dc_tcp_comm::send_loop, this, outevbase), thread::cpu_count() - 1);
      is_closed = false;
    }

    void dc_tcp_comm::construct_events() {
      libevent_init_lock.lock();
      int ret = evthread_use_pthreads();
      libevent_init_lock.unlock();
      if (ret < 0) logstream(LOG_FATAL) << "Unable to initialize libevent with pthread support!" << std::endl;
      // number of evs to create.
      outevbase = event_base_new();
      if (!outevbase) logstream(LOG_FATAL) << "Unable to construct libevent base" << std::endl;
      send_all_timeout.owner = this;
      send_all_timeout.send_all = true;
      send_triggered_timeout.owner = this;
      send_triggered_timeout.send_all = false;
      send_all_event = event_new(outevbase, -1, EV_TIMEOUT | EV_PERSIST, on_send_event, &(send_all_timeout));
      assert(send_all_event != NULL);
      struct timeval t = {SEND_POLL_TIMEOUT / 1000000, SEND_POLL_TIMEOUT %  1000000} ;
      event_add(send_all_event, &t);
      send_triggered_event = event_new(outevbase, -1, EV_TIMEOUT | EV_PERSIST, on_send_event, &(send_triggered_timeout));
      assert(send_triggered_event != NULL);

      inevbase = event_base_new();
      if (!inevbase) logstream(LOG_FATAL) << "Unable to construct libevent base" << std::endl;


      //register all event objects
      for (size_t i = 0;i < sock.size(); ++i) {
        sock[i].inevent = event_new(inevbase, sock[i].insock, EV_READ | EV_PERSIST | EV_ET,
                                     on_receive_event, &(sock[i]));
        if (sock[i].inevent == NULL) {
          logstream(LOG_FATAL) << "Unable to register socket read event" << std::endl;
        }

        sock[i].outevent = event_new(outevbase, sock[i].outsock, EV_WRITE | EV_PERSIST | EV_ET,
                                     on_send_event, &(sock[i]));
        if (sock[i].outevent == NULL) {
          logstream(LOG_FATAL) << "Unable to register socket write event" << std::endl;
        }

        event_add(sock[i].inevent, NULL);
        //struct timeval t = {0, 10};
        event_add(sock[i].outevent, NULL);
      }
    }

    size_t dc_tcp_comm::num_in_connected() const {
      size_t connected = 0;
      for (size_t i = 0;i < sock.size(); ++i) {
        connected += (sock[i].insock != -1);
      }
      return connected;
    }

    void dc_tcp_comm::trigger_send_timeout(procid_t target, bool urgent) {
      if (!urgent) {
        if (sock[target].wouldblock == false &&
            triggered_timeouts.get(target) == false) {
          triggered_timeouts.set_bit(target);
          event_active(send_triggered_event, EV_TIMEOUT, 1);
        }
      }
      else {
        // urgetnt flush. we forcefully over the sending of the socket
        // and keep force send_till_block to well. block.
        dc_tcp_comm::socket_info* sockinfo = &(sock[target]);
        sockinfo->m.lock();
        dc_tcp_comm* comm = sockinfo->owner;
        comm->check_for_new_data(*sockinfo);
        while (!sockinfo->outvec.empty()) {
          comm->send_till_block(*sockinfo);
        }
        sockinfo->m.unlock();
      }
    }


    void dc_tcp_comm::expect_close() {
      m_expect_close = true;
    }

    void dc_tcp_comm::close() {
      if (is_closed) return;
      logstream(LOG_INFO) << "Closing listening socket" << std::endl;
      // close the listening socket
      if (listensock > 0) {
        ::close(listensock);
        listensock = -1;
      }
      // shutdown the listening thread
      listenthread.join();

      // clear the outevent loop
      event_base_loopbreak(outevbase);
      outthreads.join();
      for (size_t i = 0;i < sock.size(); ++i) {
        event_free(sock[i].outevent);
      }
      event_free(send_triggered_event);
      event_free(send_all_event);
      event_base_free(outevbase);


      logstream(LOG_INFO) << "Closing outgoing sockets" << std::endl;
      // close all outgoing sockets
      for (size_t i = 0;i < sock.size(); ++i) {
        if (sock[i].outsock > 0) {
          ::close(sock[i].outsock);
          sock[i].outsock = -1;
        }
      }

      // clear the inevent loop
      event_base_loopbreak(inevbase);
      inthreads.join();
      for (size_t i = 0;i < sock.size(); ++i) {
        event_free(sock[i].inevent);
      }
      event_base_free(inevbase);


      logstream(LOG_INFO) << "Closing incoming sockets" << std::endl;
      // close all incoming sockets
      for (size_t i = 0;i < sock.size(); ++i) {
        if (sock[i].insock > 0) {
          ::close(sock[i].insock);
          sock[i].insock = -1;
        }
      }
      is_closed = true;
    }


    bool dc_tcp_comm::send_till_block(socket_info& sockinfo) {
      sockinfo.wouldblock = false;
      // while there is still data to be sent
      BEGIN_TRACEPOINT(tcp_send_call);
      while(!sockinfo.outvec.empty()) {
        sockinfo.outvec.fill_msghdr(sockinfo.data);
        ssize_t ret = sendmsg(sockinfo.outsock, &sockinfo.data, 0);
        if (ret < 0) {
          END_TRACEPOINT(tcp_send_call);
          if (errno == EWOULDBLOCK || errno == EAGAIN) {
            sockinfo.wouldblock = true;
            return false;
          }
          else {
            logstream(LOG_WARNING) << "send error: " << strerror(errno) << std::endl;
            abort();
            return false;
          }
        }

#ifdef COMM_DEBUG
        logstream(LOG_INFO) << ret << " bytes --> " << sockinfo.id << std::endl;
#endif
        network_bytessent.inc(ret);
        sockinfo.outvec.sent(ret);
#ifdef COMM_DEBUG
        logstream(LOG_INFO) << sockinfo.id << " " << sockinfo.outvec.size() << " buffers remaining" << std::endl;
#endif
      }
      END_TRACEPOINT(tcp_send_call);
      return true;
    }

    int dc_tcp_comm::sendtosock(int sockfd, const char* buf, size_t len) {
      size_t numsent = 0;
      BEGIN_TRACEPOINT(tcp_send_call);
      while (numsent < len) {
        ssize_t ret = ::send(sockfd, buf + numsent, len - numsent, 0);
        if (ret < 0) {
          logstream(LOG_ERROR) << "send error: " << strerror(errno) << std::endl;
          END_TRACEPOINT(tcp_send_call);
          return errno;
        }
        numsent += ret;
      }
      END_TRACEPOINT(tcp_send_call);
      return 0;
    }

    void dc_tcp_comm::set_tcp_no_delay(int fd) {
      int flag = 1;
      int result = setsockopt(fd,            /* socket affected */
                              IPPROTO_TCP,     /* set option at TCP level */
                              TCP_NODELAY,     /* name of option */
                              (char *) &flag,
                              sizeof(int));
      if (result < 0) {
        logstream(LOG_WARNING)
          << "Unable to disable Nagle. Performance may be signifantly reduced"
          << std::endl;
      }
      // set nonblocking
    }

    void dc_tcp_comm::set_non_blocking(int fd) {
      int flag = fcntl(fd, F_GETFL);
      if (flag < 0) {
        logstream(LOG_FATAL) << "Unable to get socket flags" << std::endl;
      }
      flag |= O_NONBLOCK;
      if (fcntl(fd, F_SETFL, flag) < 0) {
        logstream(LOG_FATAL) << "Unable to set socket as non-blocking" << std::endl;
      }

    }


    void dc_tcp_comm::new_socket(int newsock, sockaddr_in* otheraddr,
                                 procid_t id) {
      // figure out the address of the incoming connection
      uint32_t addr = *reinterpret_cast<uint32_t*>(&(otheraddr->sin_addr));
      // locate the incoming address in the list
      logstream(LOG_INFO) << "Incoming connection from "
                          << inet_ntoa(otheraddr->sin_addr) << std::endl;
      ASSERT_LT(id, all_addrs.size());
      ASSERT_EQ(all_addrs[id], addr);
      insock_lock.lock();
      ASSERT_EQ(sock[id].insock, -1);
      sock[id].insock = newsock;
      insock_cond.signal();
      insock_lock.unlock();
      logstream(LOG_INFO) << "Proc " << procid() << " accepted connection "
                          << "from machine " << id << std::endl;
    }



    void dc_tcp_comm::open_listening(int sockhandle) {
      // open listening socket
      if (sockhandle == 0) {
        listensock = socket(AF_INET, SOCK_STREAM, 0);
        // uninteresting boiler plate. Set the port number and socket type
        sockaddr_in my_addr;
        my_addr.sin_family = AF_INET;
        my_addr.sin_port = htons(portnums[curid]);
        my_addr.sin_addr.s_addr = INADDR_ANY;
        memset(&(my_addr.sin_zero), '\0', 8);
        logstream(LOG_INFO) << "Proc " << procid() << " Bind on "
                            << portnums[curid] << "\n";
        if (bind(listensock, (sockaddr*)&my_addr, sizeof(my_addr)) < 0)
          {
            logstream(LOG_FATAL) << "bind: " << strerror(errno) << "\n";
            ASSERT_TRUE(0);
          }
      }
      else {
        listensock = sockhandle;
      }
      logstream(LOG_INFO) << "Proc " << procid()
                          << " listening on " << portnums[curid] << "\n";
      ASSERT_EQ(0, listen(listensock, 128));
      // spawn a thread which loops around accept
      listenthread.launch(boost::bind(&dc_tcp_comm::accept_handler, this));
    } // end of open_listening

    void dc_tcp_comm::connect(size_t target) {
      if (sock[target].outsock != -1) {
        return;
      } else {
        int newsock = socket(AF_INET, SOCK_STREAM, 0);
        set_tcp_no_delay(newsock);
        sockaddr_in serv_addr;
        serv_addr.sin_family = AF_INET;
        // set the target port
        serv_addr.sin_port = htons(portnums[target]);
        // set the target address
        serv_addr.sin_addr = *(struct in_addr*)&(all_addrs[target]);
        memset(&(serv_addr.sin_zero), '\0', 8);
        // Connect!
        logstream(LOG_INFO) << "Trying to connect from "
                            << curid << " -> " << target
                            << " on port " << portnums[target] << "\n";
        logger(LOG_INFO, "Destination IP = %s", inet_ntoa(serv_addr.sin_addr));
        // retry 10 times at 1 second intervals
        bool success = false;
        for (size_t i = 0;i < 10; ++i) {
          if (::connect(newsock, (sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
            logstream(LOG_INFO)
              << "connect " << curid << " to " << target << ": "
              << strerror(errno) << ". Retrying...\n";
            timer::sleep(1);
            // posix says that
            /* If connect() fails, the state of the socket is unspecified.
               Conforming applications should close the file descriptor and
               create a new socket before attempting to reconnect. */
            ::close(newsock);
            newsock = socket(AF_INET, SOCK_STREAM, 0);
            set_tcp_no_delay(newsock);
          } else {
            // send the initial message
            initial_message msg; 
            msg.id = curid;
            memcpy(msg.md5, program_md5.c_str(), 32);
            sendtosock(newsock, reinterpret_cast<char*>(&msg), sizeof(initial_message));
            set_non_blocking(newsock);
            success = true;
            break;
          }
        }
        if (!success) {
          logstream(LOG_WARNING) << "Failed to establish connection" << std::endl;
          abort();
        }
        // remember the socket
        sock[target].outsock = newsock;
        logstream(LOG_INFO) << "connection from " << curid << " to " << target
                            << " established." << std::endl;
      }
    } // end of connect





////////////////////////////////////////////////////////////////////////////
//       These stuff run in seperate threads                              //
////////////////////////////////////////////////////////////////////////////

    // waits for incoming connections
    void dc_tcp_comm::accept_handler() {
      pollfd pf;
      pf.fd = listensock;
      pf.events = POLLIN;
      pf.revents = 0;
      size_t numsocks_connected = 0;
      logstream(LOG_INFO) << "Listening thread launched." << std::endl;
      while(numsocks_connected < sock.size()) {
        // wait for incoming event
        poll(&pf, 1, 1000);
        // if we have a POLLIN, we have an incoming socket request
        if (pf.revents & POLLIN) {
          logstream(LOG_INFO) << "Accepting...." << std::endl;
          // accept the socket
          sockaddr_in their_addr;
          socklen_t namelen = sizeof(sockaddr_in);
          int newsock = accept(listensock, (sockaddr*)&their_addr, &namelen);
          logstream(LOG_INFO) << "Accepted" << std::endl;
          if (newsock < 0) {
            break;
          }
          // set the socket options and inform the
          set_tcp_no_delay(newsock);
          // before accepting the socket, get the machine number
          initial_message remote_message;
          ssize_t msglen = 0;
          while(msglen != sizeof(initial_message)) {
            int retval = recv(newsock, (char*)(&remote_message) + msglen,
                           sizeof(initial_message) - msglen, 0);
            if (retval < 0) {
              if (errno == EWOULDBLOCK || errno == EAGAIN) {
                continue;
              }
              else {
                logstream(LOG_WARNING) << "error: " << errno <<  " receive error: " << strerror(errno) << std::endl;
                abort();
              }
            }
            else if (retval > 0) {
              msglen += retval;
            }
            else if (retval == 0) {
              std::cerr << "error: connection dropped." << std::endl;
              ::close(newsock);
              newsock = -1;
              exit(1);
              break;
            }
          }
          if (newsock != -1) {
            // validate the md5 hash
            std::string other_md5 = std::string(remote_message.md5, 32);
            if (other_md5 != program_md5) {
              logstream(LOG_FATAL) << "MD5 mismatch. \n "
                                   << "\tProcess " << curid << " has hash "  << program_md5 << " \n "
                                   << "\tProcess " << remote_message.id << " has hash "  << other_md5 << " \n "
                                   << "\tGraphLab requires all machines to run exactly the same binary." << std::endl;
            }
            // register the new socket
            set_non_blocking(newsock);
            new_socket(newsock, &their_addr, remote_message.id);
            ++numsocks_connected;
          }
        }
        if (listensock == -1) {
          // the owner has closed
          break;
        }
      }
      logstream(LOG_INFO) << "Listening thread quitting" << std::endl;
    } // end of run


    // libevent receive handler
    void on_receive_event(int fd, short ev, void* arg) {
      dc_tcp_comm::socket_info* sockinfo = (dc_tcp_comm::socket_info*)(arg);
      dc_tcp_comm* comm = sockinfo->owner;
      if (ev & EV_READ) {
        // get a direct pointer to my receiver
        dc_receive* receiver = comm->receiver[sockinfo->id];

        size_t buflength;
        char *c = receiver->get_buffer(buflength);
        while(1) {
          ssize_t msglen = recv(fd, c, buflength, 0);
          if (msglen < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            else {
              logstream(LOG_WARNING) << "receive error: " << strerror(errno) << std::endl;
              abort();
              break;
            }
          }
          else if (msglen == 0) {
            if (!comm->m_expect_close) {
              logstream(LOG_WARNING) << getpid() << " Unexpected socket close." << std::endl;
              abort();
            }
            // socket closed
            break;
          }
          else if (msglen > 0) {
            comm->network_bytesreceived.inc(msglen);
    #ifdef COMM_DEBUG
            logstream(LOG_INFO) << msglen << " bytes <-- "
                                << sockinfo->id  << std::endl;
    #endif
            c = receiver->advance_buffer(c, msglen, buflength);
          }
        }
      }
    }

    void dc_tcp_comm::receive_loop(struct event_base* ev) {
      logstream(LOG_INFO) << "Receive loop Started" << std::endl;
      int ret = event_base_dispatch(ev);
      if (ret != 0) {
        logstream(LOG_FATAL) << "Receive loop Quit with " << ret << std::endl;
      }
      else {
        logstream(LOG_INFO) << "Receive loop Stopped" << std::endl;
      }
    }


    void dc_tcp_comm::check_for_new_data(dc_tcp_comm::socket_info& sockinfo) {
      buffered_len.inc(sender[sockinfo.id]->get_outgoing_data(sockinfo.outvec));
    }


    inline void process_sock(dc_tcp_comm::socket_info* sockinfo) {
      if (sockinfo->m.try_lock()) {
        dc_tcp_comm* comm = sockinfo->owner;
        // get a direct pointer to my receiver
        if (sockinfo->wouldblock == false) {
          comm->check_for_new_data(*sockinfo);
          if (!sockinfo->outvec.empty()) {
            comm->send_till_block(*sockinfo);
          }
        }
        sockinfo->m.unlock();
      }
    }

    // libevent receive handler
    void on_send_event(int fd, short ev, void* arg) {
      if (ev & EV_WRITE) {
        dc_tcp_comm::socket_info* sockinfo = (dc_tcp_comm::socket_info*)(arg);
        sockinfo->wouldblock = false;
        process_sock(sockinfo);
     }
      else if (ev & EV_TIMEOUT) {
        dc_tcp_comm::timeout_event* te =  (dc_tcp_comm::timeout_event*)(arg);
        dc_tcp_comm* comm = te->owner;
        if (te->send_all == false) {
          // this is a triggered event
          foreach(uint32_t i, comm->triggered_timeouts) {
            comm->triggered_timeouts.clear_bit(i);
            dc_tcp_comm::socket_info* sockinfo = &(comm->sock[i]);
            process_sock(sockinfo);
          }
        } else {
          // send all event
          for(uint32_t i = 0;i < comm->sock.size(); ++i) {
            dc_tcp_comm::socket_info* sockinfo = &(comm->sock[i]);
            process_sock(sockinfo);
          }
        }
      }
    }


    void dc_tcp_comm::send_loop(struct event_base* ev) {
      logstream(LOG_INFO) << "Send loop Started" << std::endl;
      int ret = event_base_dispatch(ev);
      if (ret != 0) {
        logstream(LOG_FATAL) << "Send loop Quit with " << ret << std::endl;
      }
      else {
        logstream(LOG_INFO) << "Send loop Stopped" << std::endl;
      }
    }
  }; // end of namespace dc_impl
}; // end of namespace graphlab

