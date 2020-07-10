/*
 * page_compression.c
 *		Routines for page compression
 *
 * There are two implementations at the moment: zstd, and the Postgres
 * pg_lzcompress(). zstd support requires that the server was compiled
 * with --with-zstd.
 *
 * Copyright (c) 2020, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/storage/smgr/page_compression.c
 */
#include "postgres.h"
#include "miscadmin.h"
#include "catalog/pg_type.h"
#include "utils/array.h"
#include "utils/relcache.h"

#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <linux/falloc.h>
#include <sys/stat.h>
#include <assert.h>
#include <sys/mman.h>

#ifdef USE_ZSTD
#include <zstd.h>

#define DEFAULT_ZSTD_COMPRESSION_LEVEL 3
#define MIN_ZSTD_COMPRESSION_LEVEL ZSTD_minCLevel()
#define MAX_ZSTD_COMPRESSION_LEVEL 19

#endif

#include "storage/page_compression.h"
#include "common/pg_lzcompress.h"
#include "utils/datum.h"


/**
 * compress_page() -- Compress one page.
 * 
 *		Only the parts other than the page header will be compressed. The
 *		compressed data is rounded by chunck_size, and the compressed
 *		data and number of chuncks are returned. Compression needs to be
 *		able to save at least 1 chunk of space, otherwise it returns NULL.
 */
char *
compress_page(const char *src, int chunck_size, uint8 algorithm, int8 level, int *nchuncks)
{
	int 		compressed_size,targetDstSize;
	PageCompressData *pcdptr;
	char 		*dst;

	*nchuncks = 0;

	targetDstSize = BLCKSZ - chunck_size;

	if(targetDstSize < chunck_size)
		return NULL;

	switch(algorithm)
	{
		case COMPRESS_TYPE_PGLZ:
			dst = palloc(BLCKSZ + 4);
			pcdptr = (PageCompressData *)dst;

			compressed_size = pglz_compress(src + SizeOfPageHeaderData,
											BLCKSZ - SizeOfPageHeaderData,
											pcdptr->data,
											PGLZ_strategy_always);//TODO PGLZ_strategy_default? PGLZ_strategy_always
			break;
		
#ifdef USE_ZSTD
		case COMPRESS_TYPE_ZSTD:
		{
			size_t out_len = ZSTD_compressBound(BLCKSZ - SizeOfPageHeaderData);
			dst = palloc(out_len);
			pcdptr = (PageCompressData *)dst;

			if(level == 0 || level < ZSTD_minCLevel() || level > ZSTD_maxCLevel() )
				level = DEFAULT_ZSTD_COMPRESSION_LEVEL;

			compressed_size = ZSTD_compress(pcdptr->data,
							out_len,
							src + SizeOfPageHeaderData,
							BLCKSZ - SizeOfPageHeaderData,
							level);

			if (ZSTD_isError(compressed_size))
			{
				elog(WARNING, "ZSTD_compress failed: %s", ZSTD_getErrorName(compressed_size));
				pfree(dst);
				return NULL;
			}
			break;
		}
#endif
		default:
			elog(ERROR, "unrecognized compression algorithm %d",algorithm);
			break;
			
	}

	elog(DEBUG1, "compress_page() called: compressed_size=%d", compressed_size);

	if(compressed_size < 0 ||
		SizeOfPageCompressDataHeaderData + compressed_size > targetDstSize)
		{
			pfree(dst);
			return NULL;
		}

	memcpy(pcdptr->page_header, src, SizeOfPageHeaderData);
	pcdptr->size = compressed_size;

	*nchuncks = (SizeOfPageCompressDataHeaderData + compressed_size + chunck_size -1 ) / chunck_size;

	if((SizeOfPageCompressDataHeaderData + compressed_size) < chunck_size * (*nchuncks))
	{
		memset(pcdptr->data + compressed_size,
				0x00,
				chunck_size * (*nchuncks) - SizeOfPageCompressDataHeaderData - compressed_size);
	}

	return dst;
}

/**
 * decompress_page() -- Decompress one compressed page.
 * 
 *		note:The size of dst must be greater than or equal to BLCKSZ.
 */
int
decompress_page(const char * src, char *dst, uint8 algorithm)
{
	int			decompressed_size;
	PageCompressData *pcdptr;

	pcdptr = (PageCompressData *)src;

	memcpy(dst, src, SizeOfPageHeaderData);

	switch(algorithm)
	{
		case COMPRESS_TYPE_PGLZ:
			decompressed_size = pglz_decompress(pcdptr->data,
												pcdptr->size,
												dst + SizeOfPageHeaderData,
												BLCKSZ - SizeOfPageHeaderData,
												false);
			break;

#ifdef USE_ZSTD
		case COMPRESS_TYPE_ZSTD:
			decompressed_size = ZSTD_decompress(dst + SizeOfPageHeaderData,
												BLCKSZ - SizeOfPageHeaderData,
												pcdptr->data,
												pcdptr->size);

			if (ZSTD_isError(decompressed_size))
			{
				elog(WARNING, "ZSTD_decompress failed: %s", ZSTD_getErrorName(decompressed_size));
				return -1;
			}

			break;
#endif

		default:
			elog(ERROR, "unrecognized compression algorithm %d",algorithm);
			break;

	}

	return SizeOfPageHeaderData + decompressed_size;
}


/**
 * pc_mmap() -- create memory map for page compress file's address area.
 * 
 */
PageCompressHeader *
pc_mmap(int fd, int chunk_size)
{
	PageCompressHeader  *map;
	int 				file_size,pc_memory_map_size;

	pc_memory_map_size = SizeofPageCompressAddrFile(chunk_size);

	file_size = lseek(fd, 0, SEEK_END);
	if(file_size != pc_memory_map_size)
	{
		if (ftruncate(fd, pc_memory_map_size) != 0)
			return (PageCompressHeader *) MAP_FAILED;
	}

#ifdef WIN32
	{
		HANDLE		mh = CreateSnapshotMapping((HANDLE)_get_osfhandle(fd), NULL, PAGE_READWRITE,
										   0, (DWORD) pc_memory_map_size, NULL);

		if (mh == NULL)
			return (PageCompressHeader *) MAP_FAILED;

		map = (PageCompressHeader *) MapViewOfFile(mh, FILE_MAP_ALL_ACCESS, 0, 0, 0);
		CloseHandle(mh);
	}
	if (map == NULL)
		return (PageCompressHeader *) MAP_FAILED;

#else
	map = (PageCompressHeader *) mmap(NULL, pc_memory_map_size, PROT_WRITE | PROT_READ, MAP_SHARED, fd, 0);
#endif
	return map;
}

/**
 * pc_munmap() -- release memory map of page compress file.
 * 
 */
int
pc_munmap(PageCompressHeader * map)
{
#ifdef WIN32
	return UnmapViewOfFile(map) ? 0 : -1;
#else
	return munmap(map, SizeofPageCompressAddrFile(map->chunk_size));
#endif
}

/**
 * pc_msync() -- sync memory map of page compress file.
 * 
 */
int
pc_msync(PageCompressHeader *map)
{
	if (!enableFsync)
		return 0;
#ifdef WIN32
	return FlushViewOfFile(map, SizeofPageCompressAddrFile(map->chunk_size)) ? 0 : -1;
#else
	return msync(map, SizeofPageCompressAddrFile(map->chunk_size), MS_SYNC);
#endif
}


/**
 * buildCompressReloptions() -- build compression option array from PageCompressOpts
 * 
 */
Datum
buildCompressReloptions(PageCompressOpts *pcOpt)
{
	Datum		result;
	ArrayBuildState *astate;
	text		*t;
	char		*value;
	Size		len;

	/* We build new array using accumArrayResult */
	astate = NULL;

	/* compress_type */
	value = NULL;
	switch (pcOpt->compress_type)
	{
		case COMPRESS_TYPE_PGLZ:
			value = "pglz";
			break;

		case COMPRESS_TYPE_ZSTD:
			value = "zstd";
			break;
		
		default:
			break;
	}

	if(value != NULL)
	{
		len = VARHDRSZ + strlen("compress_type") + 1 + strlen(value);

		/* +1 leaves room for sprintf's trailing null */
		t = (text *) palloc(len + 1);
		SET_VARSIZE(t, len);
		sprintf(VARDATA(t), "compress_type=%s", value);

		astate = accumArrayResult(astate, PointerGetDatum(t),
									false, TEXTOID,
									CurrentMemoryContext);
	}

	/* compress_level */
	if(pcOpt->compress_level != 0)
	{
		value = psprintf("%d",pcOpt->compress_level);

		len = VARHDRSZ + strlen("compress_level") + 1 + strlen(value);

		/* +1 leaves room for sprintf's trailing null */
		t = (text *) palloc(len + 1);
		SET_VARSIZE(t, len);
		sprintf(VARDATA(t), "compress_level=%s", value);
		pfree(value);

		astate = accumArrayResult(astate, PointerGetDatum(t),
									false, TEXTOID,
									CurrentMemoryContext);
	}

	/* compress_level */
	if(pcOpt->compress_chunk_size != BLCKSZ / 2)
	{
		value = psprintf("%d",pcOpt->compress_chunk_size);

		len = VARHDRSZ + strlen("compress_chunk_size") + 1 + strlen(value);

		/* +1 leaves room for sprintf's trailing null */
		t = (text *) palloc(len + 1);
		SET_VARSIZE(t, len);
		sprintf(VARDATA(t), "compress_chunk_size=%s", value);
		pfree(value);

		astate = accumArrayResult(astate, PointerGetDatum(t),
									false, TEXTOID,
									CurrentMemoryContext);
	}

	/* compress_level */
	if(pcOpt->compress_prealloc_chunks != 0)
	{
		value = psprintf("%d",pcOpt->compress_prealloc_chunks);

		len = VARHDRSZ + strlen("compress_prealloc_chunks") + 1 + strlen(value);

		/* +1 leaves room for sprintf's trailing null */
		t = (text *) palloc(len + 1);
		SET_VARSIZE(t, len);
		sprintf(VARDATA(t), "compress_prealloc_chunks=%s", value);
		pfree(value);

		astate = accumArrayResult(astate, PointerGetDatum(t),
									false, TEXTOID,
									CurrentMemoryContext);
	}

	if (astate)
		result = makeArrayResult(astate, CurrentMemoryContext);
	else
		result = (Datum) 0;

	return result;
}
