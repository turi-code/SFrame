#ifndef SFRAME_QUERY_ENGINE_DISK_BACKED_QUEUE_HPP
#define SFRAME_QUERY_ENGINE_DISK_BACKED_QUEUE_HPP
#include <queue>
#include <string>
#include <fileio/general_fstream.hpp>
#include <fileio/temp_files.hpp>
#include <logger/assertions.hpp>

namespace graphlab {
class oarchive;
class iarchive;

template typename T>
void disk_backed_queue_serializer {
  void save(oarchive& oarc, const &T t) {
    oarc << t;
  }
  void load(iarchive& oarc, const &T t) {
    iarc >> t;
  }
};

/**
 * This class provides operators with an unbounded length cache.
 * This class is *not* thread safe.
 *
 * \tparam T Datatype to be saved. Must be serializable
 *
 * This class guarantees extremely high efficiency if the total number 
 * of elements do not exceed the cache limit.
 *
 * The implementation works by having a pair of files and a central in memory 
 * queue.
 *
 * - push file
 * - center queue
 * - pop file
 *
 * When data is pushed, it pushes into the center in memory queue until it is 
 * full then starts writing to the push_file.
 *
 * When data is popped, it starts popping from the pop_file, then pops
 * from the center queue. When the center queue is empty, the push file
 * and pop file are swapped and it pops from the pop file again.
 * 
 */
template <typename T, typename Serializer = disk_backed_queue_serializer<T>>
class disk_backed_queue {
 public:
  /**
   * Constructs a disk backed queue
   * \param cache limit Number of elements to cache
   */
  explicit disk_backed_queue(size_t cache_limit = 128, 
                             const Serializer& serializer = Serializer()):
      m_cache_limit(cache_limit), m_serializer(serializer) { 
    if (m_cache_limit == 0) m_cache_limit = 1;
    m_temp_names = {get_temp_name_prefer_hdfs("dqueue"),
                    get_temp_name_prefer_hdfs("dqueue")};
    initial_open_queues();
  }

  ~disk_backed_queue() {
    // close all streams
    if (m_push_stream) m_push_stream->close();
    if (m_pop_stream) m_pop_stream->close();
    fileio::delete_path(m_temp_names.first);
    fileio::delete_path(m_temp_names.second);
  }
  /**
   * Sets the cache limit
   * \param cache limit Number of elements to cache
   */
  size_t set_cache_limit(size_t cache_limit) {
    m_cache_limit = cache_limit;
    if (m_cache_limit == 0) m_cache_limit = 1;
  }

  /**
   * Pushes an element into the queue.
   */
  void push(const T& el) {
    if (m_writing_to_in_memory_queue) {
      if (m_in_memory_queue.size() < m_cache_limit) {
        m_in_memory_queue.push(el);
        ++m_element_count;
        return;
      }
      // in memory queue is full. switch to the push queue;
      m_writing_to_in_memory_queue = false;
    }

    oarchive oarc(*m_push_stream);
    m_serializer.save(oarc, el);
    ++m_element_count;
    ++m_stream_element_counts.first;
  }

  /**
   * Pops and element from the queue.
   * Returns true on success, false on failure (queue is empty)
   */
  bool pop(T& ret) {
    if (m_element_count == 0) return false;

    if (m_reading_from_in_memory_queue) {
      // read from in memory queue. If in memory queue is empty,
      // flip the streams and read from the input stream
      if (read_from_in_memory_queue(ret)) return true; 
      flip_queues();
      return read_from_input_stream(ret);
    } else {
      // read from input stream. If input stream is empty
      // continue reading from the in memory queue.
      if (read_from_input_stream(ret)) return true; 
      m_reading_from_in_memory_queue = true;
      return read_from_in_memory_queue(ret);
    }
  }

  /**
   * Returns the number of elements in the queue.
   */
  size_t num_elements() const {
    return m_element_count;
  }
 private:

  size_t m_cache_limit = 0;
  size_t m_element_count = 0;
  std::queue<T> m_in_memory_queue;
  Serializer m_serializer;

  /// file names of the push and pop queue respectively
  std::pair<std::string, std::string> m_temp_names;
  /// Number of elements in the push and pop queue respectively
  std::pair<size_t, size_t> m_stream_element_counts = {0, 0};
  std::shared_ptr<general_ofstream> m_push_stream;
  std::shared_ptr<general_ifstream> m_pop_stream;

  /**
   * When this is true, we write to the in memory queue. When this is false
   * we write to the output stream.
   */
  bool m_writing_to_in_memory_queue = true;

  /**
   * If this is true, we read from the in memory queue (input stream is empty)
   * when false we read from the input stream.
   */
  bool m_reading_from_in_memory_queue = true;
  
  void initial_open_queues() {
    m_push_stream = std::make_shared<general_ofstream>(m_temp_names.first);
  }

  void flip_queues() {
    DASSERT_TRUE(m_in_memory_queue.empty());
    // close all streams
    if (m_push_stream) m_push_stream->close();
    if (m_pop_stream) m_pop_stream->close();

    // swap and reopen
    std::swap(m_temp_names.first, m_temp_names.second);
    std::swap(m_stream_element_counts.first, m_stream_element_counts.second);
    m_push_stream = std::make_shared<general_ofstream>(m_temp_names.first);
    m_pop_stream = std::make_shared<general_ifstream>(m_temp_names.second);

    // push stream is now empty. so we are writing to the in memory queue.
    // pop stream is now full, so we are reading from the in memory queue
    m_writing_to_in_memory_queue = true;
    m_reading_from_in_memory_queue = false;
  }

  /// Reads an element from the in memory queue. Returns true on success.
  bool read_from_in_memory_queue(T& ret) {
    if (m_in_memory_queue.empty()) return false;
    ret = m_in_memory_queue.front();
    m_in_memory_queue.pop();
    --m_element_count;
    return true;
  }

  /// Reads an element from the input stream. Returns true on success.
  void read_from_input_stream(T& ret) {
    if (m_stream_element_counts.second == ) return false;
    iarchive iarc(*m_pop_stream);
    m_serializer.load(iarc, ret);
    --m_element_count;
    --m_stream_element_counts.second;
    return true;
  }
};

} // graphlab
#endif
