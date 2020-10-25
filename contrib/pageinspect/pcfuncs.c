/*-------------------------------------------------------------------------
 *
 * pcfuncs.c
 *	  Functions to investigate the content of address file of compressed relation
 *
 * Access-method specific inspection functions are in separate files.
 *
 * Copyright (c) 2007-2020, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/pageinspect/pcfuncs.c
 *
 *-------------------------------------------------------------------------
 */

#include <unistd.h>

#include "postgres.h"

#include "access/htup_details.h"
#include "access/relation.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pageinspect.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/varlena.h"

#include "storage/page_compression.h"


static PageCompressHeader *get_compress_address_contents_internal(text *relname, int segno);


/*
 * get_compress_address_header
 *
 */
PG_FUNCTION_INFO_V1(get_compress_address_header);

Datum
get_compress_address_header(PG_FUNCTION_ARGS)
{
	text	   *relname = PG_GETARG_TEXT_PP(0);
	uint32		segno = PG_GETARG_UINT32(1);
	Datum		result;
	HeapTuple	tuple;
	TupleDesc	tupleDesc;
	Datum		values[5];
	bool		nulls[5];
	PageCompressHeader *pcMap;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use raw page functions")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	pcMap = get_compress_address_contents_internal(relname, segno);
	if(pcMap == NULL)
		PG_RETURN_NULL();

	values[0] = UInt32GetDatum(pg_atomic_read_u32(&pcMap->nblocks));
	values[1] = UInt32GetDatum(pg_atomic_read_u32(&pcMap->allocated_chunks));
	values[2] = UInt32GetDatum(pcMap->chunk_size);
	values[3] = UInt32GetDatum(pcMap->algorithm);
	values[4] = UInt32GetDatum(pg_atomic_read_u32(&pcMap->last_synced_nblocks));
	values[5] = UInt32GetDatum(pg_atomic_read_u32(&pcMap->last_synced_allocated_chunks));

	pfree(pcMap);

	memset(nulls, 0, sizeof(nulls));

	/* Build and return the result tuple */
	tuple = heap_form_tuple(tupleDesc, values, nulls);
	result = HeapTupleGetDatum(tuple);

	PG_RETURN_DATUM(result);
}

/*
 * get_compress_address_items
 *
 * Allows inspection of compress address contents of a compressed relation.
 */
PG_FUNCTION_INFO_V1(get_compress_address_items);

typedef struct compress_address_items_state
{
	TupleDesc	tupd;
	uint32		blkno;
	PageCompressHeader *pcMap;
} compress_address_items_state;

Datum
get_compress_address_items(PG_FUNCTION_ARGS)
{
	text	   *relname = PG_GETARG_TEXT_PP(0);
	uint32		segno = PG_GETARG_UINT32(1);
	compress_address_items_state *inter_call_data = NULL;
	FuncCallContext *fctx;
	PageCompressHeader *pcMap;
	PageCompressAddr *pcAddr;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use raw page functions")));

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc	tupdesc;
		MemoryContext mctx;
		uint32		blkno;

		fctx = SRF_FIRSTCALL_INIT();
		mctx = MemoryContextSwitchTo(fctx->multi_call_memory_ctx);

		inter_call_data = palloc(sizeof(compress_address_items_state));

		/* Build a tuple descriptor for our result type */
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			elog(ERROR, "return type must be a row type");

		inter_call_data->tupd = tupdesc;
		inter_call_data->blkno = 0;

		pcMap = get_compress_address_contents_internal(relname, segno);

		inter_call_data->pcMap = pcMap;
		if(pcMap)
			{
				/* find the largest page with a non-empty address */
				fctx->max_calls = pg_atomic_read_u32(&pcMap->nblocks);
				for(blkno = fctx->max_calls; blkno < RELSEG_SIZE; blkno++)
				{
					pcAddr = GetPageCompressAddr(pcMap, 
									 pcMap->chunk_size,
									 blkno);
					if(pcAddr->allocated_chunks != 0)
						fctx->max_calls = blkno + 1;
				}
			}
		else
			fctx->max_calls = 0;

		fctx->user_fctx = inter_call_data;

		MemoryContextSwitchTo(mctx);
	}

	fctx = SRF_PERCALL_SETUP();
	inter_call_data = fctx->user_fctx;

	if (fctx->call_cntr < fctx->max_calls)
	{
		Datum		result;
		HeapTuple	tuple;
		int			i, len, chunk_count;
		char	   *values[5];
		char		buf[256];
		char		*p;

		pcMap = inter_call_data->pcMap;
		pcAddr = GetPageCompressAddr(pcMap, 
									 pcMap->chunk_size,
									 inter_call_data->blkno);

		/* Extract information from the compress address */

		p = buf;
		snprintf(p,sizeof(buf) - (p - buf), "{");
		p++;

		/* skip invalid chunkno at tail */ 
		chunk_count = BLCKSZ / pcMap->chunk_size;
		while(pcAddr->chunknos[chunk_count - 1] == 0)
			chunk_count --;

		for(i=0; i< chunk_count; i++)
		{
			if(i==0)
				len = snprintf(p, sizeof(buf) - (p - buf), "%d", pcAddr->chunknos[i]);
			else
				len = snprintf(p, sizeof(buf) - (p - buf), ",%d", pcAddr->chunknos[i]);
			p += len;
		}
		snprintf(p, sizeof(buf) - (p - buf), "}");

		values[0] = psprintf("%d", inter_call_data->blkno);
		values[1] = psprintf("%d", pcAddr->nchunks);
		values[2] = psprintf("%d", pcAddr->allocated_chunks);
		values[3] = psprintf("%s", buf);

		/* Build and return the result tuple. */
		tuple = BuildTupleFromCStrings(TupleDescGetAttInMetadata(inter_call_data->tupd),
																		values);

		result = HeapTupleGetDatum(tuple);

		inter_call_data->blkno++;

		SRF_RETURN_NEXT(fctx, result);
	}
	else
		SRF_RETURN_DONE(fctx);
}

/*
 * get_compress_address_contents_internal
 * 
 * returns raw compress address file contents
 */
static PageCompressHeader *
get_compress_address_contents_internal(text *relname, int segno)
{
	RangeVar   *relrv;
	Relation	rel;
	PageCompressHeader *pcMap;
	PageCompressHeader *result = NULL;
	int			i;

	relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
	rel = relation_openrv(relrv, AccessShareLock);

	/* Check that this relation support compression */
	if (rel->rd_rel->relkind != RELKIND_RELATION && rel->rd_rel->relkind != RELKIND_INDEX)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot get compress address contents from relation \"%s\", only support table and index",
						RelationGetRelationName(rel))));

	/*
	 * Reject attempts to read non-local temporary relations; we would be
	 * likely to get wrong data since we have no visibility into the owning
	 * session's local buffers.
	 */
	if (RELATION_IS_OTHER_TEMP(rel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot access temporary tables of other sessions")));

	if(rel->rd_node.compress_chunk_size != 0 && rel->rd_node.compress_algorithm != 0)
	{
		/* Get file path */
		char	*path, *pca_path;
		int		file, chunk_size;

		path = relpathbackend(rel->rd_node, rel->rd_backend, MAIN_FORKNUM);
		if (segno > 0)
			pca_path = psprintf("%s.%u_pca", path, segno);
		else
			pca_path = psprintf("%s_pca", path);
		pfree(path);

		file = open(pca_path, PG_BINARY | O_RDONLY, 0);

		if (file < 0)
			ereport(ERROR,
			(errcode_for_file_access(),
				errmsg("could not open file \"%s\": %m", pca_path)));

		chunk_size = rel->rd_node.compress_chunk_size;
		pcMap = pc_mmap(file, chunk_size, true);
		if(pcMap == MAP_FAILED)
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
						errmsg("Failed to mmap page compression address file %s: %m",
								pca_path)));

		result = (PageCompressHeader *)palloc(SizeofPageCompressAddrFile(chunk_size));

		/* Make a member-by-member copy to ensure that each member is read atomically */
		for(i=0; i < SizeofPageCompressAddrFile(chunk_size)/sizeof(int); i++)
			((int *)result)[i] = ((int *)pcMap)[i];

		pc_munmap(pcMap);
		close(file);
		pfree(pca_path);
	}

	relation_close(rel, AccessShareLock);

	return result;
}


/*
 * page_compress
 *
 * compress one page
 */

PG_FUNCTION_INFO_V1(page_compress);

Datum
page_compress(PG_FUNCTION_ARGS)
{
	bytea	   *raw_page = PG_GETARG_BYTEA_P(0);
	char	   *algorithm_name = text_to_cstring(PG_GETARG_TEXT_PP(1));
	int32		level = PG_GETARG_INT32(2);
	char		*work_buffer;
	int			raw_page_size;
	int			work_buffer_size;
	int8		algorithm;
	bytea	   *result;
	int			nbytes;
	PageHeader	page;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use raw page functions")));

	raw_page_size = VARSIZE(raw_page) - VARHDRSZ;

	/*
	 * Check that enough data was supplied, so that we don't try to access
	 * fields outside the supplied buffer.
	 */
	if (raw_page_size < BLCKSZ)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("input page too small (%d bytes)", raw_page_size)));

	page = (PageHeader) VARDATA(raw_page);

	/* compress page */
	if (strcmp(algorithm_name, "pglz") == 0)
		algorithm = COMPRESS_ALGORITHM_PGLZ;
	else if (strcmp(algorithm_name, "zstd") == 0)
		algorithm = COMPRESS_ALGORITHM_ZSTD;
	else
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("unrecognized compression algorithm %s",
						algorithm_name)));

	work_buffer_size = compress_page_buffer_bound(algorithm);
	if(work_buffer_size < 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("unrecognized compression algorithm %d",
						algorithm)));
	work_buffer = palloc(work_buffer_size);

	nbytes = compress_page((char *)page, work_buffer, work_buffer_size, algorithm, level);

	if(nbytes < 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("unrecognized compression algorithm %d",
						algorithm)));

	result = (bytea *) palloc(nbytes + VARHDRSZ);
	SET_VARSIZE(result, nbytes + VARHDRSZ);
	memcpy(VARDATA(result), work_buffer, nbytes);

	pfree(work_buffer);

	PG_RETURN_BYTEA_P(result);
}
