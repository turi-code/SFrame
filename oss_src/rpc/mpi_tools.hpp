/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
/**
 * Copyright (c) 2009 Carnegie Mellon University.
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


#ifndef GRAPHLAB_MPI_TOOLS
#define GRAPHLAB_MPI_TOOLS

#ifdef HAS_MPI
#include <mpi.h>
#endif

#include <vector>

#include <boost/iostreams/device/array.hpp>
#include <boost/iostreams/stream.hpp>

#include <serialization/serialization_includes.hpp>
#include <graphlab/util/charstream.hpp>
#include <network/net_util.hpp>






#include <graphlab/macros_def.hpp>

// Get rid of unused variable warnings for this code that show up in
// release mode
#ifdef NDEBUG

#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wunused-but-set-variable"
#endif

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-variable"
#pragma clang diagnostic ignored "-Wunused-but-set-variable"
#endif

#endif

namespace graphlab {
  namespace mpi_tools {


    /**
     * The init function is used to initialize MPI and must be called
     * to clean the command line arguments.
     */
    inline void init(int& argc, char**& argv) {
#ifdef HAS_MPI
      const int required(MPI_THREAD_SINGLE);
      int provided(-1);
      int error = MPI_Init_thread(&argc, &argv, required, &provided);
      assert(provided >= required);
      assert(error == MPI_SUCCESS);
#else
      logstream(LOG_EMPH) << "MPI Support was not compiled." << std::endl;
#endif
    } // end of init

    inline void finalize() {
#ifdef HAS_MPI
      int error = MPI_Finalize();
      assert(error == MPI_SUCCESS);
#endif
    } // end of finalize


    inline bool initialized() {
#ifdef HAS_MPI
      int ret_value = 0;
      int error = MPI_Initialized(&ret_value);
      assert(error == MPI_SUCCESS);
      return ret_value;
#else
      return false;
#endif
    } // end of initialized

    inline size_t rank() {
#ifdef HAS_MPI
      int mpi_rank(-1);
      MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
      assert(mpi_rank >= 0);
      return size_t(mpi_rank);
#else
      return 0;
#endif
    }

    inline size_t size() {
#ifdef HAS_MPI
      int mpi_size(-1);
      MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);
      assert(mpi_size >= 0);
      return size_t(mpi_size);
#else
      return 1;
#endif
    }



    template<typename T>
    void all_gather(const T& elem, std::vector<T>& results) {
#ifdef HAS_MPI
      // Get the mpi rank and size
      size_t mpi_size(size());
      if(results.size() != mpi_size) results.resize(mpi_size);

      // Serialize the local map
      graphlab::charstream cstrm(128);
      graphlab::oarchive oarc(cstrm);
      oarc << elem;
      cstrm.flush();
      char* send_buffer = cstrm->c_str();
      int send_buffer_size = (int)cstrm->size();
      assert(send_buffer_size >= 0);

      // compute the sizes
      std::vector<int> recv_sizes(mpi_size, -1);
      // Compute the sizes
      int error = MPI_Allgather(&send_buffer_size,  // Send buffer
                                1,                  // send count
                                MPI_INT,            // send type
                                &(recv_sizes[0]),  // recvbuffer
                                1,                  // recvcount
                                MPI_INT,           // recvtype
                                MPI_COMM_WORLD);
      assert(error == MPI_SUCCESS);
      for(size_t i = 0; i < recv_sizes.size(); ++i)
        assert(recv_sizes[i] >= 0);


      // Construct offsets
      std::vector<int> recv_offsets(recv_sizes);
      int sum = 0, tmp = 0;
      for(size_t i = 0; i < recv_offsets.size(); ++i) {
        tmp = recv_offsets[i]; recv_offsets[i] = sum; sum += tmp;
      }

      // if necessary realloac recv_buffer
      std::vector<char> recv_buffer(sum);

      // recv all the maps
      error = MPI_Allgatherv(send_buffer,         // send buffer
                             send_buffer_size,    // how much to send
                             MPI_BYTE,            // send type
                             &(recv_buffer[0]),   // recv buffer
                             &(recv_sizes[0]),    // amount to recv
                                                  // for each cpuess
                             &(recv_offsets[0]),  // where to place data
                             MPI_BYTE,
                             MPI_COMM_WORLD);
      assert(error == MPI_SUCCESS);
      // Update the local map
      namespace bio = boost::iostreams;
      typedef bio::stream<bio::array_source> icharstream;
      icharstream strm(&(recv_buffer[0]), recv_buffer.size());
      graphlab::iarchive iarc(strm);
      for(size_t i = 0; i < results.size(); ++i) {
        iarc >> results[i];
      }
#else
      logstream(LOG_FATAL) << "MPI not installed!" << std::endl;
#endif
    } // end of mpi all gather




    template<typename T>
    void all2all(const std::vector<T>& send_data,
                 std::vector<T>& recv_data) {
#ifdef HAS_MPI
      // Get the mpi rank and size
      size_t mpi_size(size());
      ASSERT_EQ(send_data.size(), mpi_size);
      if(recv_data.size() != mpi_size) recv_data.resize(mpi_size);

      // Serialize the output data and compute buffer sizes
      graphlab::charstream cstrm(128);
      graphlab::oarchive oarc(cstrm);
      std::vector<int> send_buffer_sizes(mpi_size);
      for(size_t i = 0; i < mpi_size; ++i) {
        const size_t OLD_SIZE(cstrm->size());
        oarc << send_data[i];
        cstrm.flush();
        const size_t ELEM_SIZE(cstrm->size() - OLD_SIZE);
        send_buffer_sizes[i] = ELEM_SIZE;
      }
      cstrm.flush();
      char* send_buffer = cstrm->c_str();
      std::vector<int> send_offsets(send_buffer_sizes);
      int total_send = 0;
      for(size_t i = 0; i < send_offsets.size(); ++i) {
        const int tmp = send_offsets[i];
        send_offsets[i] = total_send;
        total_send += tmp;
      }

      // AlltoAll scatter the buffer sizes
      std::vector<int> recv_buffer_sizes(mpi_size);
      int error = MPI_Alltoall(&(send_buffer_sizes[0]),
                               1,
                               MPI_INT,
                               &(recv_buffer_sizes[0]),
                               1,
                               MPI_INT,
                               MPI_COMM_WORLD);
      ASSERT_EQ(error, MPI_SUCCESS);

      // Construct offsets
      std::vector<int> recv_offsets(recv_buffer_sizes);
      int total_recv = 0;
      for(size_t i = 0; i < recv_offsets.size(); ++i){
        const int tmp = recv_offsets[i];
        recv_offsets[i] = total_recv;
        total_recv += tmp;
      }
      // Do the massive send
      std::vector<char> recv_buffer(total_recv);
      error = MPI_Alltoallv(send_buffer,
                            &(send_buffer_sizes[0]),
                            &(send_offsets[0]),
                            MPI_BYTE,
                            &(recv_buffer[0]),
                            &(recv_buffer_sizes[0]),
                            &(recv_offsets[0]),
                            MPI_BYTE,
                            MPI_COMM_WORLD);
      ASSERT_EQ(error, MPI_SUCCESS);

      // Deserialize the result
      namespace bio = boost::iostreams;
      typedef bio::stream<bio::array_source> icharstream;
      icharstream strm(&(recv_buffer[0]), recv_buffer.size());
      graphlab::iarchive iarc(strm);
      for(size_t i = 0; i < recv_data.size(); ++i) {
        iarc >> recv_data[i];
      }
#else
      logstream(LOG_FATAL) << "MPI not installed!" << std::endl;
#endif
    } // end of mpi all to all







    /**
     * called on the root.  must be matched with gather(const T& elem);
     */
    template<typename T>
    void gather(size_t root, const T& elem) {
#ifdef HAS_MPI
       // Get the mpi rank and size
      assert(root < size_t(std::numeric_limits<int>::max()));
      int mpi_root(root);

      // Serialize the local map
      graphlab::charstream cstrm(128);
      graphlab::oarchive oarc(cstrm);
      oarc << elem;
      cstrm.flush();
      char* send_buffer = cstrm->c_str();
      int send_buffer_size = cstrm->size();
      assert(send_buffer_size >= 0);

      // compute the sizes
      // Compute the sizes
      int error = MPI_Gather(&send_buffer_size,  // Send buffer
                             1,                  // send count
                             MPI_INT,            // send type
                             NULL,               // recvbuffer
                             1,                  // recvcount
                             MPI_INT,           // recvtype
                             mpi_root,          // root rank
                             MPI_COMM_WORLD);
      assert(error == MPI_SUCCESS);


      // recv all the maps
      error = MPI_Gatherv(send_buffer,         // send buffer
                          send_buffer_size,    // how much to send
                          MPI_BYTE,            // send type
                          NULL,                // recv buffer
                          NULL,                // amount to recv
                                               // for each cpuess
                          NULL,                // where to place data
                          MPI_BYTE,
                          mpi_root,            // root rank
                          MPI_COMM_WORLD);
      assert(error == MPI_SUCCESS);
#else
      logstream(LOG_FATAL) << "MPI not installed!" << std::endl;
#endif
    } // end of gather






    /**
     * called on the root.  must be matched with gather(const T& elem);
     */
    template<typename T>
    void gather(const T& elem, std::vector<T>& results) {
#ifdef HAS_MPI
      // Get the mpi rank and size
      size_t mpi_size(size());
      int mpi_rank(rank());
      if(results.size() != mpi_size) results.resize(mpi_size);

      // Serialize the local map
      graphlab::charstream cstrm(128);
      graphlab::oarchive oarc(cstrm);
      oarc << elem;
      cstrm.flush();
      char* send_buffer = cstrm->c_str();
      int send_buffer_size = cstrm->size();
      assert(send_buffer_size >= 0);

      // compute the sizes
      std::vector<int> recv_sizes(mpi_size, -1);
      // Compute the sizes
      int error = MPI_Gather(&send_buffer_size,  // Send buffer
                             1,                  // send count
                             MPI_INT,            // send type
                             &(recv_sizes[0]),  // recvbuffer
                             1,                  // recvcount
                             MPI_INT,           // recvtype
                             mpi_rank,          // root rank
                             MPI_COMM_WORLD);
      assert(error == MPI_SUCCESS);
      for(size_t i = 0; i < recv_sizes.size(); ++i)
        assert(recv_sizes[i] >= 0);


      // Construct offsets
      std::vector<int> recv_offsets(recv_sizes);
      int sum = 0, tmp = 0;
      for(size_t i = 0; i < recv_offsets.size(); ++i) {
        tmp = recv_offsets[i]; recv_offsets[i] = sum; sum += tmp;
      }

      // if necessary realloac recv_buffer
      std::vector<char> recv_buffer(sum);

      // recv all the maps
      error = MPI_Gatherv(send_buffer,         // send buffer
                          send_buffer_size,    // how much to send
                          MPI_BYTE,            // send type
                          &(recv_buffer[0]),   // recv buffer
                          &(recv_sizes[0]),    // amount to recv
                                               // for each cpuess
                          &(recv_offsets[0]),  // where to place data
                          MPI_BYTE,
                          mpi_rank,            // root rank
                          MPI_COMM_WORLD);
      assert(error == MPI_SUCCESS);
      // Update the local map
      namespace bio = boost::iostreams;
      typedef bio::stream<bio::array_source> icharstream;
      icharstream strm(&(recv_buffer[0]), recv_buffer.size());
      graphlab::iarchive iarc(strm);
      for(size_t i = 0; i < results.size(); ++i) {
        iarc >> results[i];
      }
#else
      logstream(LOG_FATAL) << "MPI not installed!" << std::endl;
#endif
    } // end of gather



    /**
     * called on the root.  must be matched with gather(const T& elem);
     */
    template<typename T>
    void bcast(const size_t& root, T& elem) {
#ifdef HAS_MPI
      // Get the mpi rank and size
      if(mpi_tools::rank() == root) {
        // serialize the object
        graphlab::charstream cstrm(128);
        graphlab::oarchive oarc(cstrm);
        oarc << elem;
        cstrm.flush();
        char* send_buffer = cstrm->c_str();
        int send_buffer_size = cstrm->size();
        assert(send_buffer_size >= 0);

        // send the ammount to send
        int error = MPI_Bcast(&send_buffer_size,  // Send buffer
                              1,                  // send count
                              MPI_INT,            // send type
                              root,               // root rank
                              MPI_COMM_WORLD);
        assert(error == MPI_SUCCESS);

        // send the actual data
        error = MPI_Bcast(send_buffer,  // Send buffer
                          send_buffer_size,    // send count
                          MPI_BYTE,            // send type
                          root,               // root rank
                          MPI_COMM_WORLD);
        assert(error == MPI_SUCCESS);

      } else {
        int recv_buffer_size(-1);
        // recv the ammount the required buffer size
        int error = MPI_Bcast(&recv_buffer_size,  // recvbuffer
                              1,                  // recvcount
                              MPI_INT,            // recvtype
                              root,               // root rank
                              MPI_COMM_WORLD);
        assert(error == MPI_SUCCESS);
        assert(recv_buffer_size >= 0);

        std::vector<char> recv_buffer(recv_buffer_size);
        error = MPI_Bcast(&(recv_buffer[0]),  // recvbuffer
                          recv_buffer_size,                  // recvcount
                          MPI_BYTE,            // recvtype
                          root,               // root rank
                          MPI_COMM_WORLD);
        assert(error == MPI_SUCCESS);
        // construct the local element
        namespace bio = boost::iostreams;
        typedef bio::stream<bio::array_source> icharstream;
        icharstream strm(&(recv_buffer[0]), recv_buffer.size());
        graphlab::iarchive iarc(strm);
        iarc >> elem;

      }
#else
      logstream(LOG_FATAL) << "MPI not installed!" << std::endl;
#endif
    } // end of bcast



    template<typename T>
    void send(const T& elem, const size_t id, const int tag = 0) {
#ifdef HAS_MPI
      // Get the mpi rank and size
      assert(id < size());
      // Serialize the local map
      graphlab::charstream cstrm(128);
      graphlab::oarchive oarc(cstrm);
      oarc << elem;
      cstrm.flush();
      char* send_buffer = cstrm->c_str();
      int send_buffer_size = cstrm->size();
      assert(send_buffer_size >= 0);

      int dest(id);
      // send the size
      int error = MPI_Send(&send_buffer_size,  // Send buffer
                           1,                  // send count
                           MPI_INT,            // send type
                           dest,               // destination
                           tag,                  // tag
                           MPI_COMM_WORLD);
      assert(error == MPI_SUCCESS);

      // send the actual content
      error = MPI_Send(send_buffer,         // send buffer
                       send_buffer_size,    // how much to send
                       MPI_BYTE,            // send type
                       dest,
                       tag,
                       MPI_COMM_WORLD);
      assert(error == MPI_SUCCESS);
#else
      logstream(LOG_FATAL) << "MPI not installed!" << std::endl;
#endif
    } // end of send



    template<typename T>
    void recv(T& elem, const size_t id, const int tag = 0) {
#ifdef HAS_MPI
      // Get the mpi rank and size
      assert(id < size());

      int recv_buffer_size(-1);
      int dest(id);
      MPI_Status status;
      // recv the size
      int error = MPI_Recv(&recv_buffer_size,
                           1,
                           MPI_INT,
                           dest,
                           tag,
                           MPI_COMM_WORLD,
                           &status);
      assert(error == MPI_SUCCESS);
      assert(recv_buffer_size > 0);

      std::vector<char> recv_buffer(recv_buffer_size);
      // recv the actual content
      error = MPI_Recv(&(recv_buffer[0]),
                       recv_buffer_size,
                       MPI_BYTE,
                       dest,
                       tag,
                       MPI_COMM_WORLD,
                       &status);
      assert(error == MPI_SUCCESS);
      // deserialize
      // Update the local map
      namespace bio = boost::iostreams;
      typedef bio::stream<bio::array_source> icharstream;
      icharstream strm(&(recv_buffer[0]), recv_buffer.size());
      graphlab::iarchive iarc(strm);
      iarc >> elem;
#else
      logstream(LOG_FATAL) << "MPI not installed!" << std::endl;
#endif
    }




    void get_master_ranks(std::set<size_t>& master_ranks);





  }; // end of namespace mpi tools
}; //end of graphlab namespace

// Reenable the unused variable warnings 
#ifdef NDEBUG

#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif

#ifdef __clang__
#pragma clang diagnostic pop
#endif

#endif

#include <graphlab/macros_undef.hpp>

#endif

