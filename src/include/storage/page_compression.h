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
#include "datatype/timestamp.h"

#ifdef FRONTEND
typedef uint32 pg_atomic_uint32;
#else
#include "port/atomics.h"
#include "utils/rel.h"

/* The page compression feature relies on native atomic operation support.
 * On platforms that do not support native atomic operations, the members
 * of pg_atomic_uint32 contain semaphore objects, which will affect the 
 * persistence of compressed page address files.
 */
#define SUPPORT_PAGE_COMPRESSION (sizeof(pg_atomic_uint32) == sizeof(uint32))
#endif

/* In order to avoid the inconsistency of address metadata data when the server
 * is down, it is necessary to prevent the address metadata of one data block
 * from crossing two storage device blocks. The block size of ordinary storage
 * devices is a multiple of 512, so 512 is used as the block size of the
 * compressed address file. 
 */
#define COMPRESS_ADDR_BLCKSZ 512

/* COMPRESS_ALGORITHM_XXX must be the same as COMPRESS_TYPE_XXX */
#define COMPRESS_ALGORITHM_PGLZ 1
#define COMPRESS_ALGORITHM_ZSTD 2

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
	pg_atomic_uint32	last_synced_nblocks;	/* last synced nblocks */
	pg_atomic_uint32	last_synced_allocated_chunks;	/* last synced allocated_chunks */
	TimestampTz			last_recovery_start_time; 	/* postmaster start time of last recovery */
} PageCompressHeader;

typedef struct PageCompressAddr
{
	volatile uint8		nchunks;			/* number of chunks for this block */
	volatile uint8		allocated_chunks;	/* number of allocated chunks for this block */

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
	(SizeOfPageCompressAddrHeaderData + sizeof(pc_chunk_number_t) * (BLCKSZ / (chunk_size)))

#define NumberOfPageCompressAddrPerBlock(chunk_size) \
	(COMPRESS_ADDR_BLCKSZ / SizeOfPageCompressAddr(chunk_size))

#define OffsetOfPageCompressAddr(chunk_size, blockno) \
	(COMPRESS_ADDR_BLCKSZ * (1 + (blockno) / NumberOfPageCompressAddrPerBlock(chunk_size)) \
	+ SizeOfPageCompressAddr(chunk_size) * ((blockno) % NumberOfPageCompressAddrPerBlock(chunk_size)))

#define GetPageCompressAddr(pcbuffer, chunk_size, blockno) \
	(PageCompressAddr *)((char *)(pcbuffer) + OffsetOfPageCompressAddr((chunk_size),(blockno) % RELSEG_SIZE))

#define SizeofPageCompressAddrFile(chunk_size) \
	OffsetOfPageCompressAddr((chunk_size), RELSEG_SIZE)

#define OffsetOfPageCompressChunk(chunk_size, chunkno) \
	((chunk_size) * ((chunkno) - 1))

#define MAX_PAGE_COMPRESS_ADDRESS_FILE_SIZE SizeofPageCompressAddrFile(BLCKSZ / 8)

/* Abnormal scenarios may cause holes in the space allocation of data files,
 * causing data file expansion. Usually the holes are not too big, so the definition
 * allows a maximum of 10,000 chunks for holes. If allocated_chunks exceeds this value,
 * VACUUM FULL needs to be executed to reclaim space.
 */
#define MAX_CHUNK_NUMBER(chunk_size) (RELSEG_SIZE * (BLCKSZ / (chunk_size)) + 10000)

/* Compress function */
extern int compress_page_buffer_bound(uint8 algorithm);
extern int compress_page(const char *src, char *dst, int dst_size, uint8 algorithm, int8 level);
extern int decompress_page(const char * src, char *dst, uint8 algorithm);

/* Memory mapping function */
extern PageCompressHeader * pc_mmap(int fd, int chunk_size, bool readonly);
extern int pc_munmap(PageCompressHeader * map);
extern int pc_msync(PageCompressHeader * map);


#ifndef FRONTEND
extern int compress_address_flush_chunks;

/* compression options function */
extern Datum buildCompressReloptions(PageCompressOpts *pcOpt);

extern void check_and_repair_compress_address(PageCompressHeader *pcMap, uint16 chunk_size, uint8 algorithm, const char *path);
#endif

#endif							/* PAGE_COMPRESSION_H */
