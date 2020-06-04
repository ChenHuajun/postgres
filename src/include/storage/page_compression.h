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

#include "storage/bufpage.h"
#include "port/atomics.h"
#include "utils/rel.h"

typedef uint32 pc_chunk_number_t;

/*
 * layout of Page Compress file:
 *
 * - PageCompressHeader
 * - PageCompressAddr[]
 * - chuncks of PageCompressData
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
	(PageCompressAddr *)((char *)pcbuffer + OffsetOfPageCompressAddr(chunk_size,(blockno) / RELSEG_SIZE))

#define OffsetOfFirstPageCompressChunck(chunk_size) \
	(((OffsetOfPageCompressAddr(chunk_size, RELSEG_SIZE) + BLCKSZ - 1)/ BLCKSZ) * BLCKSZ)

#define OffsetOfPageCompressChunk(chunk_size, chunkno) \
	(OffsetOfFirstPageCompressChunck(chunk_size) + (chunk_size) * (chunkno - 1))

#define SizeofPageCompressMemoryMapArea(chunk_size) OffsetOfFirstPageCompressChunck(chunk_size)


/* Compress function */
extern char *compress_page(const char *src, int chunck_size, uint8 algorithm, int *nchuncks);
extern int decompress_page(const char * src, char *dst, uint8 algorithm);

/* Memory mapping function */
extern PageCompressHeader * pc_mmap(int fd, int chunk_size);
extern int pc_munmap(PageCompressHeader * map);
extern int pc_msync(PageCompressHeader * map);

#endif							/* PAGE_COMPRESSION_H */
