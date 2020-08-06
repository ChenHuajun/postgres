/*-------------------------------------------------------------------------
 *
 * file_ops.h
 *	  Helper functions for operating on files
 *
 * Copyright (c) 2013-2020, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef FILE_OPS_H
#define FILE_OPS_H

#include "filemap.h"

extern void open_target_file(const char *path, bool trunc);
extern void write_target_range(char *buf, off_t begin, size_t size);
extern void close_target_file(void);
extern void remove_target_file(const char *path, bool missing_ok);
extern void truncate_target_file(const char *path, off_t newsize);
extern void create_target(file_entry_t *t);
extern void remove_target(file_entry_t *t);

extern char *slurpFile(const char *datadir, const char *path, size_t *filesize);

extern void open_target_compressed_relation(const char *path);
extern void write_target_compressed_relation_chunk(char *buf, size_t size, int blocknum, int chunknum,
                                                    int nchunks, int prealloc_chunks);
extern void truncate_target_compressed_relation(const char *path, int newblocks);

#endif							/* FILE_OPS_H */
