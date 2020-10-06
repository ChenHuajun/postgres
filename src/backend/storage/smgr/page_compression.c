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
#include "utils/datum.h"
#include "utils/relcache.h"

#include "utils/timestamp.h"
#include "storage/bufmgr.h"
#include "storage/page_compression.h"
#include "storage/page_compression_impl.h"

int compress_address_flush_chunks;

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

void
check_and_repair_compress_address(PageCompressHeader *pcMap, uint16 chunk_size, uint8 algorithm, const char *path)
{
	int i, unused_chunks;
	BlockNumber blocknum, max_blocknum, max_nonzero_blocknum;
	uint32 nblocks, allocated_chunks;
	pc_chunk_number_t max_allocated_chunkno;
	BlockNumber *global_chunknos;
	char last_recovery_start_time_buf[sizeof(TimestampTz)];
	char start_time_buf[sizeof(TimestampTz)];
	bool need_check = false;

	unused_chunks = 0;
	max_blocknum = (BlockNumber)-1;
	max_nonzero_blocknum = (BlockNumber)-1;
	max_allocated_chunkno = (pc_chunk_number_t)0;

	/* if the relation had been checked in this startup, skip */
	memcpy(last_recovery_start_time_buf, &pcMap->last_recovery_start_time,sizeof(TimestampTz));
	memcpy(start_time_buf, &PgStartTime,sizeof(TimestampTz));
	for(i=0; i < sizeof(TimestampTz); i++)
	{
		if(start_time_buf[i] != last_recovery_start_time_buf[i])
		{
			need_check = true;
			break;
		}
	}
	if(!need_check)
		return;

	/* check head of compress address file */
	if(pcMap->chunk_size != chunk_size || pcMap->algorithm != algorithm)
	{
		/* reinitialize compress head if it's invalid and zero_damaged_pages is on */
		if(zero_damaged_pages)
		{
			ereport(WARNING,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("invalid chunk_size %u or algorithm %u in head of compress relation address file \"%s\", and reinitialized it.",
							 pcMap->chunk_size, pcMap->algorithm, path)));

			pcMap->algorithm = algorithm;
			pg_atomic_write_u32(&pcMap->nblocks, RELSEG_SIZE);
			pg_atomic_write_u32(&pcMap->allocated_chunks, 0);
			pg_atomic_write_u32(&pcMap->last_synced_allocated_chunks, 0);
			pcMap->chunk_size = chunk_size;
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("invalid chunk_size %u or algorithm %u in head of compress relation address file \"%s\"",
							 pcMap->chunk_size, pcMap->algorithm, path)));
	}

	nblocks = pg_atomic_read_u32(&pcMap->nblocks);
	allocated_chunks = pg_atomic_read_u32(&pcMap->allocated_chunks);
	global_chunknos = palloc0(MAX_CHUNK_NUMBER(chunk_size));

	/* check compress address of every pages */
	for(blocknum = 0; blocknum < (BlockNumber)RELSEG_SIZE; blocknum++)
	{
		PageCompressAddr *pcAddr = GetPageCompressAddr(pcMap, chunk_size, blocknum);

		/* skip when found first zero filled block after nblocks */
		if(blocknum >= (BlockNumber)nblocks && pcAddr->allocated_chunks == 0)
			break;

		/* check allocated_chunks for one page */
		if(pcAddr->allocated_chunks > BLCKSZ / chunk_size)
		{
			if(zero_damaged_pages)
			{
				MemSet(pcAddr, 0, SizeOfPageCompressAddr(chunk_size));
				ereport(WARNING,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("invalid allocated_chunks %u of block %u in file \"%s\", and zero this block",
								 pcAddr->allocated_chunks, blocknum, path)));
				continue;
			}
			else
			{
				pfree(global_chunknos);
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("invalid allocated_chunks %u of block %u in file \"%s\"",
								 pcAddr->allocated_chunks, blocknum, path)));
			}
		}

		/* check chunknos for one page */
		for(i = 0; i< pcAddr->allocated_chunks; i++)
		{
			/* check for invalid chunkno */
			if(pcAddr->chunknos[i] == 0 || pcAddr->chunknos[i] > MAX_CHUNK_NUMBER(chunk_size))
			{
				if (zero_damaged_pages)
				{
					MemSet(pcAddr, 0, SizeOfPageCompressAddr(chunk_size));
					ereport(WARNING,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("invalid chunk number %u of block %u in file \"%s\", and zero this block",
									 pcAddr->chunknos[i], blocknum, path)));
					continue;
				}
				else
				{
					pfree(global_chunknos);
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("invalid chunk number %u of block %u in file \"%s\"",
									 pcAddr->chunknos[i], blocknum, path)));
				}
			}

			/* check for duplicate chunkno */
			if(global_chunknos[pcAddr->chunknos[i] - 1] !=0 )
			{
				if (zero_damaged_pages)
				{
					MemSet(pcAddr, 0, SizeOfPageCompressAddr(chunk_size));
					ereport(WARNING,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("chunk number %u of block %u duplicate with block %u in file \"%s\", and zero this block",
									 pcAddr->chunknos[i], blocknum, global_chunknos[pcAddr->chunknos[i] - 1], path)));
					continue;
				}
				else
				{
					pfree(global_chunknos);
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("chunk number %u of block %u duplicate with block %u in file \"%s\"",
									 pcAddr->chunknos[i], blocknum, global_chunknos[pcAddr->chunknos[i]], path)));
				}
			}
		}

		/* clean chunknos beyond allocated_chunks for one page */
		for(i = pcAddr->allocated_chunks; i< BLCKSZ / chunk_size; i++)
		{
			if(pcAddr->chunknos[i] != 0)
			{
				pcAddr->chunknos[i] = 0;
				ereport(WARNING,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("clear chunk number %u beyond allocated_chunks %u of block %u in file \"%s\"",
								 pcAddr->chunknos[i], pcAddr->allocated_chunks, blocknum, path)));
			}
		}

		/* check nchunks for one page */
		if(pcAddr->nchunks > pcAddr->allocated_chunks)
		{
			if(zero_damaged_pages)
			{
				MemSet(pcAddr, 0, SizeOfPageCompressAddr(chunk_size));
				ereport(WARNING,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("nchunks %u exceeds allocated_chunks %u of block %u in file \"%s\", and zero this block",
								 pcAddr->nchunks, pcAddr->allocated_chunks, blocknum, path)));
				continue;
			}
			else
			{
				pfree(global_chunknos);
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("nchunks %u exceeds allocated_chunks %u of block %u in file \"%s\"",
								 pcAddr->nchunks, pcAddr->allocated_chunks, blocknum, path)));	
			}
		}

		global_chunknos[pcAddr->chunknos[i] -1 ] = blocknum;
		if(blocknum > max_blocknum)
			max_blocknum = blocknum;

		if(pcAddr->nchunks >0 && blocknum > max_nonzero_blocknum)
			max_nonzero_blocknum = blocknum;

		for(i = 0; i< pcAddr->allocated_chunks; i++)
		{
			if(pcAddr->chunknos[i] > max_allocated_chunkno)
				max_allocated_chunkno = pcAddr->chunknos[i];
		}
	}

	/* check for holes in allocated chunks*/
	for(i = 0; i < max_allocated_chunkno; blocknum++)
		if(global_chunknos[i] == 0)
			unused_chunks ++;

	if(unused_chunks > 0)
		ereport(WARNING,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("there are %u chunks of total allocated chunks %u can not be use in file \"%s\"",
						unused_chunks, max_allocated_chunkno, path),
				 errhint("You may need to run VACUMM FULL to optimize space allocation, or run REINDEX if it is an index.")));

	/* update head of compress file */
	if(nblocks != (max_nonzero_blocknum + 1) ||
	   allocated_chunks != max_allocated_chunkno)
	{
		pg_atomic_write_u32(&pcMap->nblocks, max_nonzero_blocknum + 1);
		pg_atomic_write_u32(&pcMap->last_synced_nblocks, max_nonzero_blocknum + 1);
		pg_atomic_write_u32(&pcMap->allocated_chunks, max_allocated_chunkno);
		pg_atomic_write_u32(&pcMap->last_synced_allocated_chunks, max_allocated_chunkno);

		ereport(WARNING,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("update head of compress file \"%s\". old allocated_chunks/nblocks %u/%u, new allocated_chunks/nblocks %u/%u",
						 path, allocated_chunks, nblocks, max_allocated_chunkno, max_nonzero_blocknum + 1)));
	}

	/* clean compress address after max_blocknum + 1 */
	for(blocknum = max_blocknum + 1; blocknum < (BlockNumber)RELSEG_SIZE; blocknum++)
	{
		char buf[128], *p;
		PageCompressAddr *pcAddr = GetPageCompressAddr(pcMap, chunk_size, blocknum);

		/* skip zero block */
		if(pcAddr->allocated_chunks == 0 && pcAddr->nchunks == 0)
			continue;

		/* clean compress address and output content of the address */
		MemSet(buf, 0, sizeof(buf));
		p = buf;

		for(i = 0; i< pcAddr->allocated_chunks; i++)
		{
			if(pcAddr->chunknos[i])
			{
				if(i==0)
					snprintf(p, (sizeof(buf) - (p - buf)), "%u", pcAddr->chunknos[i]);
				else
					snprintf(p, (sizeof(buf) - (p - buf)), ",%u", pcAddr->chunknos[i]);
				p += strlen(p);
			}
		}

		MemSet(pcAddr, 0, SizeOfPageCompressAddr(chunk_size));
		ereport(WARNING,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("clean unused compress address of block %u in file \"%s\", old allocated_chunks/nchunks/chunknos: %u/%u/{%s}",
						 blocknum, path, pcAddr->allocated_chunks, pcAddr->nchunks, buf)));
	}

	pfree(global_chunknos);

	if(pc_msync(pcMap) != 0)
		ereport(data_sync_elevel(ERROR),
				(errcode_for_file_access(),
					errmsg("could not msync file \"%s\": %m",
						path)));

	pcMap->last_recovery_start_time = PgStartTime;
}