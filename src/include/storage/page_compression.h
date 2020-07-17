/*
 * page_compression.h
 *		internal declarations for page compression
 *
 * Copyright (c) 2020, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/storage/page_compression.h
 */

#ifndef PAGE_COMPRESSION_H
#define PAGE_COMPRESSION_H

#include <sys/mman.h>

#include "storage/bufpage.h"
//#include "utils/rel.h"

typedef struct pg_atomic_uint32_native_impl
{
	volatile uint32 value;
} pg_atomic_uint32_native_impl;

#ifdef FRONTEND
typedef pg_atomic_uint32_native_impl pg_atomic_uint32;
#else
#include "port/atomics.h"
#include "utils/rel.h"

/* The page compression feature relies on native atomic operation support.
 * On platforms that do not support native atomic operations, the members
 * of pg_atomic_uint32 contain semaphore objects, which will affect the 
 * persistence of compressed page address files.
 */
#define SUPPORT_PAGE_COMPRESSION (sizeof(pg_atomic_uint32) == sizeof(pg_atomic_uint32_native_impl))
#endif

typedef uint32 pc_chunk_number_t;

/*
 * layout of files for Page Compress:
 *
 * 1. page compression address file(_pca)
 * - PageCompressHeader
 * - PageCompressAddr[]
 *
 * 2. page compression data file(_pcd)
 * - PageCompressData[]
 *
 */

typedef struct PageCompressHeader
{
	pg_atomic_uint32	nblocks;	/* number of total blocks in this segment */
	pg_atomic_uint32	allocated_chunks;	/* number of total allocated chunks in data area */
	uint16				chunk_size;	/* size of each chunk, must be 1/2 1/4 or 1/8 of BLCKSZ */
	uint8				algorithm;	/* compress algorithm, 1=pglz, 2=lz4 */
} PageCompressHeader;

typedef struct PageCompressAddr
{
	uint8				nchunks;			/* number of chunks for this block */
	uint8				allocated_chunks;	/* number of allocated chunks for this block */

	/* variable-length fields, 1 based chunk no array for this block, size of the array must be 2, 4 or 8 */
	pc_chunk_number_t	chunknos[FLEXIBLE_ARRAY_MEMBER];
} PageCompressAddr;

typedef struct PageCompressData
{
	char	page_header[SizeOfPageHeaderData];	/* page header */
	uint32	size;								/* size of compressed data */
	char	data[FLEXIBLE_ARRAY_MEMBER];		/* compressed page, except for the page header */
} PageCompressData;


#define SizeOfPageCompressHeaderData sizeof(PageCompressHeader)
#define SizeOfPageCompressAddrHeaderData offsetof(PageCompressAddr, chunknos)
#define SizeOfPageCompressDataHeaderData offsetof(PageCompressData, data)

#define SizeOfPageCompressAddr(chunk_size) \
	(SizeOfPageCompressAddrHeaderData + sizeof(pc_chunk_number_t) * BLCKSZ / (chunk_size))

#define OffsetOfPageCompressAddr(chunk_size,blockno) \
	(MAXALIGN(SizeOfPageCompressHeaderData) + SizeOfPageCompressAddr(chunk_size) * (blockno))

#define GetPageCompressAddr(pcbuffer,chunk_size,blockno) \
	(PageCompressAddr *)((char *)pcbuffer + OffsetOfPageCompressAddr(chunk_size,(blockno) % RELSEG_SIZE))

#define SizeofPageCompressAddrFile(chunk_size) \
	(((OffsetOfPageCompressAddr(chunk_size, RELSEG_SIZE) + BLCKSZ - 1)/ BLCKSZ) * BLCKSZ)

#define OffsetOfPageCompressChunk(chunk_size, chunkno) \
	((chunk_size) * (chunkno - 1))


/* Compress function */
extern int compress_page_buffer_bound(uint8 algorithm);
extern int compress_page(const char *src, char *dst, int dst_size, uint8 algorithm, int8 level);
extern int decompress_page(const char * src, char *dst, uint8 algorithm);

/* Memory mapping function */
extern PageCompressHeader * pc_mmap(int fd, int chunk_size, bool readonly);
extern int pc_munmap(PageCompressHeader * map);
extern int pc_msync(PageCompressHeader * map);


#ifndef FRONTEND
/* compression options function */
extern Datum buildCompressReloptions(PageCompressOpts *pcOpt);
#endif

#endif							/* PAGE_COMPRESSION_H */
