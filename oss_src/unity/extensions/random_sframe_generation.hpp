#ifndef GRAPHLAB_UNITY_RANDOM_SFRAME_GENERATION_H_
#define GRAPHLAB_UNITY_RANDOM_SFRAME_GENERATION_H_

#include <unity/lib/gl_sarray.hpp>
#include <string>

graphlab::gl_sframe _generate_random_sframe(size_t num_rows, std::string column_types,
                                            size_t random_seed, bool generate_target, double noise_level);

graphlab::gl_sframe _generate_random_classification_sframe(size_t n_rows, std::string column_types,
                                                           size_t _random_seed, size_t num_classes,
                                                           size_t num_extra_class_bins, double noise_level);

#endif /* GRAPHLAB_UNITY_RANDOM_SFRAME_GENERATION_H_ */
