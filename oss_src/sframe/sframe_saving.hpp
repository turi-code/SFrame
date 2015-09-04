/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_SFRAME_SAVING_HPP
#define GRAPHLAB_SFRAME_SAVING_HPP
namespace graphlab {
class sframe;
/**
 * Saves an SFrame to another index file location using the most naive method:
 * decode rows, and write them
 */
void sframe_save_naive(const sframe& sf, 
                       std::string index_file);

/**
 * Saves an SFrame to another index file location using a more efficient method,
 * block by block.
 */
void sframe_save_blockwise(const sframe& sf, 
                           std::string index_file);

/**
 * Automatically determines the optimal strategy to save an sframe
 */
void sframe_save(const sframe& sf, 
                 std::string index_file);

/**
 * Performs an "incomplete save" to a target index file location.
 * All this ensures is that the sframe's contents are located on the 
 * same "file-system" (protocol) as the index file. Essentially the reference
 * save is guaranteed to be valid for only as long as no other SFrame files are
 * deleted.
 *
 * Essentially this can be used to build a "delta" SFrame.
 * - You already have an SFrame on disk somewhere. Say... /data/a
 * - You open it and add a column
 * - Calling sframe_save_weak_reference to save it to /data/b
 * - The saved SFrame in /data/b will include just the new column, but reference
 * /data/a for the remaining columns.
 *
 * \param sf The SFrame to save
 * \param index_file The output file location
 * 
 */
void sframe_save_weak_reference(const sframe& sf,
                                std::string index_file);

}; // naemspace graphlab

#endif
