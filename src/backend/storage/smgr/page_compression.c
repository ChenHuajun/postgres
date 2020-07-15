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

#include "storage/page_compression.h"
#include "storage/page_compression_impl.h"


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