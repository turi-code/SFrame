#include <globals/globals.hpp>
#include <export.hpp>

namespace graphlab {

/** The timeout for connecting to a lambda worker, in seconds.
 *
 *  Set to 0 to attempt one try and exit immediately on failure.
 *
 *  Set to -1 to disable timeout completely.
 */
EXPORT double LAMBDA_WORKER_CONNECTION_TIMEOUT = 60;

REGISTER_GLOBAL(double, LAMBDA_WORKER_CONNECTION_TIMEOUT, true)

}
