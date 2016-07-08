/*
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#include <boost/program_options.hpp>
#include <cppipc/server/comm_server.hpp>
#include <logger/logger.hpp>
#include <lambda/rcpplambda.hpp>
#include <process/process_util.hpp>
#include <parallel/thread_pool.hpp>

#include <RInside.h>

#ifdef HAS_TCMALLOC
#include <google/malloc_extension.h>
#endif

namespace po = boost::program_options;

using namespace graphlab;

#ifdef HAS_TCMALLOC
/**
 *  If TCMalloc is available, we try to release memory back to the
 *  system every 15 seconds or so. TCMalloc is otherwise somewhat...
 *  aggressive about keeping memory around.
 */
static bool stop_memory_release_thread = false;
graphlab::mutex memory_release_lock;
graphlab::conditional memory_release_cond;

void memory_release_loop() {
    memory_release_lock.lock();
    while (!stop_memory_release_thread) {
        memory_release_cond.timedwait(memory_release_lock, 15);
        MallocExtension::instance()->ReleaseFreeMemory();
    }
    memory_release_lock.unlock();
}

#endif

void print_help(std::string program_name, po::options_description& desc) {
    std::cerr << "Lambda Server\n"
              << desc << "\n"
              << "Example: " << program_name << " ipc:///tmp/lambda_worker\n"
              << "Example: " << program_name << " tcp://127.0.0.1:10020\n"
              << "Example: " << program_name << " tcp://*:10020\n"
              << "Example: " << program_name << " tcp://127.0.0.1:10020 tcp://127.0.0.1:10021\n"
              << "Example: " << program_name << " ipc:///tmp/unity_test_server --auth_token=secretkey\n"
              << "Example: " << program_name << " ipc:///tmp/unity_test_server ipc:///tmp/unity_status secretkey\n";
}

int main(int argc, char** argv) {
    size_t parent_pid = get_parent_pid();
    // Options
    std::string program_name = argv[0];
    std::string server_address;

    boost::program_options::variables_map vm;
    po::options_description desc("Allowed options");
    desc.add_options()
    ("help", "Print this help message.")
    ("server_address",
     po::value<std::string>(&server_address)->implicit_value(server_address),
     "This must be a valid ZeroMQ endpoint and "
     "is the address the server listens on");

    po::positional_options_description positional;
    positional.add("server_address", 1);

    // try to parse the command line options
    try {
        po::command_line_parser parser(argc, argv);
        parser.options(desc);
        parser.positional(positional);
        po::parsed_options parsed = parser.run();
        po::store(parsed, vm);
        po::notify(vm);
    } catch(std::exception& error) {
        std::cout << "Invalid syntax:\n"
                  << "\t" << error.what()
                  << "\n\n" << std::endl
                  << "Description:"
                  << std::endl;
        print_help(program_name, desc);
        return 1;
    }

    if(vm.count("help")) {
        print_help(program_name, desc);
        return 0;
    }

    // initialize a new R instance and pass to unity server
    RInside * R = new RInside(0, 0);

    cppipc::comm_server server(std::vector<std::string>(), "", server_address);

    // pass a R instance to unity server
    server.register_type<graphlab::lambda::lambda_evaluator_interface>([&]() {
        return new graphlab::lambda::rcpplambda_evaluator(R);
    });

    server.start();

#ifdef HAS_TCMALLOC
    graphlab::thread memory_release_thread;
    memory_release_thread.launch(memory_release_loop);
#endif

    wait_for_parent_exit(parent_pid);

#ifdef HAS_TCMALLOC
    stop_memory_release_thread = true;
    memory_release_cond.signal();
    memory_release_thread.join();
#endif


}

