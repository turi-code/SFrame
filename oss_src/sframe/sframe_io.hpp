/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_SFRAME_IO_HPP
#define GRAPHLAB_SFRAME_IO_HPP

#include<flexible_type/flexible_type.hpp>
#include<serialization/oarchive.hpp>

class JSONNode;

namespace graphlab {

/**
 * Write a csv string of a vector of flexible_types (as a row in the sframe) to buffer.
 * Return the number of bytes written.
 */
size_t sframe_row_to_csv(const std::vector<flexible_type>& row, char* buf, size_t buflen);

/**
 * Write column_names and column_values (as a row in the sframe) to JSONNode.
 */
void sframe_row_to_json(const std::vector<std::string>& column_names,
                        const std::vector<flexible_type>& column_values,
                        JSONNode& node);
}

#endif
