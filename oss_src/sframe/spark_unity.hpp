#ifndef GRAPHLAB_SPARK_UNITY_H_
#define GRAPHLAB_SPARK_UNITY_H_

namespace graphlab { namespace spark_interface {

namespace bpo = boost::program_options;

extern flexible_type (*read_flex_obj)(const std::string& s); 
extern void (*write_all_rows)(const sframe& sf, size_t row_start, size_t row_end);

int _spark_unity_main(int argc, const char **argv);

}}


#endif /* _SPARK_UNITY_H_ */
