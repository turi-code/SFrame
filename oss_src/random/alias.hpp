/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef GRAPHLAB_RANDOM_ALIAS_HPP
#define GRAPHLAB_RANDOM_ALIAS_HPP

#include <vector>
#include <string>
#include <utility>

namespace graphlab { namespace random {

/**
 * This is a standard algorithm for sampling from probability mass functions.
 * It is also known as the Walker Method.
 * Typically sampling from general discrete functions requires the inverse CDF 
 * method, which means each sample is O(K) where K is the number of outcomes in
 * the pmf. The alias method, on the other hand, requires O(K) setup but requires
 * only O(1) for each sample. To be specific, each sample only requires a 
 * uniformly generated float and a uniformly generated integer.
 *
 * For more details, see
 * http://www.cs.toronto.edu/~gdahl/papers/aliasMethod.pdf
 * http://luc.devroye.org/chapter_three.pdf, p. 107
 */
class alias_sampler {
 public:
   alias_sampler() = default;
   alias_sampler(const alias_sampler&) = default;
   
  /**
   * Constructor. 
   *
   * \param p Vector representing the probability mass function with K
   * outcomes, where K is the size of the vector. Values should be
   * nonnegative, but they need not sum to 1.
   */
   explicit alias_sampler(const std::vector<double>& p);

  /**
   * Sample from the PMF using the alias method.
   *
   * \returns an integer between 0 and K-1, where K is the number of outcomes
   * in the provided pmf. A value i is returned with probability
   *
   * p_i / sum_{j=0}^{K-1} p_j
   */
  size_t sample();

 private:
  std::vector<size_t> J;
  std::vector<double> q;
  size_t K;
};

} // end of random 
} // end of graphlab


#endif

