/*-------------------------------------------------------------------------
 *
 * libpq_fetch.c
 *	  Functions for fetching files from a remote server.
 *
 * Copyright (c) 2013-2020, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <sys/stat.h>
#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>

#include "catalog/pg_type_d.h"
#include "datapagemap.h"
#include "fe_utils/connect.h"
#include "fetch.h"
#include "file_ops.h"
#include "filemap.h"
#include "pg_rewind.h"
#include "port/pg_bswap.h"
#include "storage/page_compression.h"


typedef union PageCompressAddrBuffer
{
	char		data[MAX_PAGE_COMPRESS_ADDRESS_FILE_SIZE];
	double		force_align_d;
	int64		force_align_i64;
} PageCompressAddrBuffer;

PGconn			*conn = NULL;
static PageCompressAddrBuffer	pca_buffer;
/*
 * Files are fetched max CHUNKSIZE bytes at a time.
 *
 * (This only applies to files that are copied in whole, or for truncated
 * files where we copy the tail. Relation files, where we know the individual
 * blocks that need to be fetched, are fetched in BLCKSZ chunks.)
 */
#define CHUNKSIZE 1000000

static void receiveFileChunks(const char *sql);
static void receiveCompressedRelationAddressFileChunks(filemap_t *map, const char *sql);
static void execute_pagemap(datapagemap_t *pagemap, const char *path,
							bool iscompressedrel, int chunk_size,
							int prealloc_chunks, compressedpagemap_t *first_compressedpagemap);
static char *run_simple_query(const char *sql);
static void run_simple_command(const char *sql);
static void fetch_compressed_relation_range(const char *path, int chunk_size, 
											BlockNumber blocknum, int nblocks,
											int prealloc_chunks,
											compressedpagemap_t *first_compressedpagemap);

void
libpqConnect(const char *connstr)
{
	char	   *str;
	PGresult   *res;

	conn = PQconnectdb(connstr);
	if (PQstatus(conn) == CONNECTION_BAD)
		pg_fatal("could not connect to server: %s",
				 PQerrorMessage(conn));

	if (showprogress)
		pg_log_info("connected to server");

	/* disable all types of timeouts */
	run_simple_command("SET statement_timeout = 0");
	run_simple_command("SET lock_timeout = 0");
	run_simple_command("SET idle_in_transaction_session_timeout = 0");

	res = PQexec(conn, ALWAYS_SECURE_SEARCH_PATH_SQL);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		pg_fatal("could not clear search_path: %s",
				 PQresultErrorMessage(res));
	PQclear(res);

	/*
	 * Check that the server is not in hot standby mode. There is no
	 * fundamental reason that couldn't be made to work, but it doesn't
	 * currently because we use a temporary table. Better to check for it
	 * explicitly than error out, for a better error message.
	 */
	str = run_simple_query("SELECT pg_is_in_recovery()");
	if (strcmp(str, "f") != 0)
		pg_fatal("source server must not be in recovery mode");
	pg_free(str);

	/*
	 * Also check that full_page_writes is enabled.  We can get torn pages if
	 * a page is modified while we read it with pg_read_binary_file(), and we
	 * rely on full page images to fix them.
	 */
	str = run_simple_query("SHOW full_page_writes");
	if (strcmp(str, "on") != 0)
		pg_fatal("full_page_writes must be enabled in the source server");
	pg_free(str);

	/*
	 * Although we don't do any "real" updates, we do work with a temporary
	 * table. We don't care about synchronous commit for that. It doesn't
	 * otherwise matter much, but if the server is using synchronous
	 * replication, and replication isn't working for some reason, we don't
	 * want to get stuck, waiting for it to start working again.
	 */
	run_simple_command("SET synchronous_commit = off");
}

/*
 * Runs a query that returns a single value.
 * The result should be pg_free'd after use.
 */
static char *
run_simple_query(const char *sql)
{
	PGresult   *res;
	char	   *result;

	res = PQexec(conn, sql);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		pg_fatal("error running query (%s) on source server: %s",
				 sql, PQresultErrorMessage(res));

	/* sanity check the result set */
	if (PQnfields(res) != 1 || PQntuples(res) != 1 || PQgetisnull(res, 0, 0))
		pg_fatal("unexpected result set from query");

	result = pg_strdup(PQgetvalue(res, 0, 0));

	PQclear(res);

	return result;
}

/*
 * Runs a command.
 * In the event of a failure, exit immediately.
 */
static void
run_simple_command(const char *sql)
{
	PGresult   *res;

	res = PQexec(conn, sql);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		pg_fatal("error running query (%s) in source server: %s",
				 sql, PQresultErrorMessage(res));

	PQclear(res);
}

/*
 * Calls pg_current_wal_insert_lsn() function
 */
XLogRecPtr
libpqGetCurrentXlogInsertLocation(void)
{
	XLogRecPtr	result;
	uint32		hi;
	uint32		lo;
	char	   *val;

	val = run_simple_query("SELECT pg_current_wal_insert_lsn()");

	if (sscanf(val, "%X/%X", &hi, &lo) != 2)
		pg_fatal("unrecognized result \"%s\" for current WAL insert location", val);

	result = ((uint64) hi) << 32 | lo;

	pg_free(val);

	return result;
}

/*
 * Get a list of all files in the data directory.
 */
void
libpqProcessFileList(void)
{
	char		sqlbuf[1024];
	PGresult   *res;
	const char *sql;
	int			i;

	/*
	 * Create a recursive directory listing of the whole data directory.
	 *
	 * The WITH RECURSIVE part does most of the work. The second part gets the
	 * targets of the symlinks in pg_tblspc directory.
	 *
	 * XXX: There is no backend function to get a symbolic link's target in
	 * general, so if the admin has put any custom symbolic links in the data
	 * directory, they won't be copied correctly.
	 */
	sql =
		"WITH RECURSIVE files (path, filename, size, isdir) AS (\n"
		"  SELECT '' AS path, filename, size, isdir FROM\n"
		"  (SELECT pg_ls_dir('.', true, false) AS filename) AS fn,\n"
		"        pg_stat_file(fn.filename, true) AS this\n"
		"  UNION ALL\n"
		"  SELECT parent.path || parent.filename || '/' AS path,\n"
		"         fn, this.size, this.isdir\n"
		"  FROM files AS parent,\n"
		"       pg_ls_dir(parent.path || parent.filename, true, false) AS fn,\n"
		"       pg_stat_file(parent.path || parent.filename || '/' || fn, true) AS this\n"
		"       WHERE parent.isdir = 't'\n"
		")\n"
		"SELECT path || filename, size, isdir,\n"
		"       pg_tablespace_location(pg_tablespace.oid) AS link_target,\n"
		"       CASE WHEN filename ~ '_pca$' THEN pg_read_binary_file(path || filename, 0, %d, true)\n"
		"            ELSE NULL\n"
		"       END AS pchdr\n"
		"FROM files\n"
		"LEFT OUTER JOIN pg_tablespace ON files.path = 'pg_tblspc/'\n"
		"                             AND oid::text = files.filename\n";
	snprintf(sqlbuf, sizeof(sqlbuf), sql, sizeof(PageCompressHeader));
	res = PQexec(conn, sqlbuf);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		pg_fatal("could not fetch file list: %s",
				 PQresultErrorMessage(res));

	/* sanity check the result set */
	if (PQnfields(res) != 5)
		pg_fatal("unexpected result set while fetching file list");

	/* Read result to local variables */
	for (i = 0; i < PQntuples(res); i++)
	{
		char	   *path = PQgetvalue(res, i, 0);
		int64		filesize = atol(PQgetvalue(res, i, 1));
		bool		isdir = (strcmp(PQgetvalue(res, i, 2), "t") == 0);
		char	   *link_target = PQgetvalue(res, i, 3);
		file_type_t type;

		if (PQgetisnull(res, i, 1))
		{
			/*
			 * The file was removed from the server while the query was
			 * running. Ignore it.
			 */
			continue;
		}

		if (link_target[0])
			type = FILE_TYPE_SYMLINK;
		else if (isdir)
			type = FILE_TYPE_DIRECTORY;
		else
			type = FILE_TYPE_REGULAR;

		if(!PQgetisnull(res, i, 4))
		{
			PageCompressHeader	*pchdr;
			size_t				length = 0;
			char			*strvalue = PQgetvalue(res, i, 4);

			pchdr = (PageCompressHeader	*)PQunescapeBytea((const unsigned char*)strvalue, &length);

			if(length == sizeof(PageCompressHeader))
				process_source_file(path, type, filesize, link_target, pchdr);
			else
				process_source_file(path, type, filesize, link_target, NULL);
			
			if(pchdr)
				PQfreemem(pchdr);
		}
		else
			process_source_file(path, type, filesize, link_target, NULL);
	}
	PQclear(res);
}

/*----
 * Runs a query, which returns pieces of files from the remote source data
 * directory, and overwrites the corresponding parts of target files with
 * the received parts. The result set is expected to be of format:
 *
 * path		text	-- path in the data directory, e.g "base/1/123"
 * begin	int8	-- offset within the file
 * chunk	bytea	-- file content
 *----
 */
static void
receiveCompressedRelationAddressFileChunks(filemap_t *map, const char *sql)
{
	PGresult   *res;

	if (PQsendQueryParams(conn, sql, 0, NULL, NULL, NULL, NULL, 1) != 1)
		pg_fatal("could not send query: %s", PQerrorMessage(conn));

	pg_log_debug("getting compressed relation address file chunks");

	if (PQsetSingleRowMode(conn) != 1)
		pg_fatal("could not set libpq connection to single row mode");

	while ((res = PQgetResult(conn)) != NULL)
	{
		char	   *filename;
		int			filenamelen;
		int64		chunkoff;
		char		chunkoff_str[32];
		int			chunksize;
		char	   *chunk;

		switch (PQresultStatus(res))
		{
			case PGRES_SINGLE_TUPLE:
				break;

			case PGRES_TUPLES_OK:
				PQclear(res);
				continue;		/* final zero-row result */

			default:
				pg_fatal("unexpected result while fetching remote files: %s",
						 PQresultErrorMessage(res));
		}

		/* sanity check the result set */
		if (PQnfields(res) != 3 || PQntuples(res) != 1)
			pg_fatal("unexpected result set size while fetching remote files");

		if (PQftype(res, 0) != TEXTOID ||
			PQftype(res, 1) != INT8OID ||
			PQftype(res, 2) != BYTEAOID)
		{
			pg_fatal("unexpected data types in result set while fetching remote files: %u %u %u",
					 PQftype(res, 0), PQftype(res, 1), PQftype(res, 2));
		}

		if (PQfformat(res, 0) != 1 &&
			PQfformat(res, 1) != 1 &&
			PQfformat(res, 2) != 1)
		{
			pg_fatal("unexpected result format while fetching remote files");
		}

		if (PQgetisnull(res, 0, 0) ||
			PQgetisnull(res, 0, 1))
		{
			pg_fatal("unexpected null values in result while fetching remote files");
		}

		if (PQgetlength(res, 0, 1) != sizeof(int64))
			pg_fatal("unexpected result length while fetching remote files");

		/* Read result set to local variables */
		memcpy(&chunkoff, PQgetvalue(res, 0, 1), sizeof(int64));
		chunkoff = pg_ntoh64(chunkoff);
		chunksize = PQgetlength(res, 0, 2);

		filenamelen = PQgetlength(res, 0, 0);
		filename = pg_malloc(filenamelen + 1);
		memcpy(filename, PQgetvalue(res, 0, 0), filenamelen);
		filename[filenamelen] = '\0';

		chunk = PQgetvalue(res, 0, 2);

		/*
		 * If a file has been deleted on the source, remove it on the target
		 * as well.  Note that multiple unlink() calls may happen on the same
		 * file if multiple data chunks are associated with it, hence ignore
		 * unconditionally anything missing.  If this file is not a relation
		 * data file, then it has been already truncated when creating the
		 * file chunk list at the previous execution of the filemap.
		 */
		if (PQgetisnull(res, 0, 2))
		{
			pg_log_debug("received null value for chunk for file \"%s\", file has been deleted",
						 filename);
			// TODO  remove_target_file(filename, true);
			pg_free(filename);
			PQclear(res);
			continue;
		}

		/*
		 * Separate step to keep platform-dependent format code out of
		 * translatable strings.
		 */
		snprintf(chunkoff_str, sizeof(chunkoff_str), INT64_FORMAT, chunkoff);
		pg_log_debug("received chunk for file \"%s\", offset %s, size %d",
					 filename, chunkoff_str, chunksize);

		/* fill compressed relation address in filemap */
		filename[strlen(filename) - strlen("_pca")] = '\0';


		memset(pca_buffer.data, 0x00, sizeof(PageCompressAddrBuffer));
		memcpy(pca_buffer.data + chunkoff, chunk, chunksize);

		fill_compressed_relation_address(NULL, filename, (PageCompressHeader *)pca_buffer.data);

		pg_free(filename);

		PQclear(res);
	}
}


/*----
 * Runs a query, which returns pieces of files from the remote source data
 * directory, and overwrites the corresponding parts of target files with
 * the received parts. The result set is expected to be of format:
 *
 * path		text	-- path in the data directory, e.g "base/1/123"
 * begin	int8	-- offset within the file
 * chunk	bytea	-- file content
 * iscompressedchunk	bool	-- if this is a chunk for compressed relation data file
 * blocknum	int4	-- block number of this compressed chunk
 * chunknum	int4	-- chunk number of this compressed chunk
 * nchunks	int4	-- number of chunks for this block
 * prealloc_chunks	int4	-- prealloc_chunks for this relation
 *----
 */
static void
receiveFileChunks(const char *sql)
{
	PGresult   *res;

	if (PQsendQueryParams(conn, sql, 0, NULL, NULL, NULL, NULL, 1) != 1)
		pg_fatal("could not send query: %s", PQerrorMessage(conn));

	pg_log_debug("getting file chunks");

	if (PQsetSingleRowMode(conn) != 1)
		pg_fatal("could not set libpq connection to single row mode");

	while ((res = PQgetResult(conn)) != NULL)
	{
		char	   *filename;
		int			filenamelen;
		int64		chunkoff;
		int			chunksize;
		char	   *chunk;
		bool		iscompressedchunk;

		switch (PQresultStatus(res))
		{
			case PGRES_SINGLE_TUPLE:
				break;

			case PGRES_TUPLES_OK:
				PQclear(res);
				continue;		/* final zero-row result */

			default:
				pg_fatal("unexpected result while fetching remote files: %s",
						 PQresultErrorMessage(res));
		}

		/* sanity check the result set */
		if (PQnfields(res) != 8 || PQntuples(res) != 1)
			pg_fatal("unexpected result set size while fetching remote files");

		if (PQftype(res, 0) != TEXTOID ||
			PQftype(res, 1) != INT8OID ||
			PQftype(res, 2) != BYTEAOID ||
			PQftype(res, 3) != BOOLOID ||
			PQftype(res, 4) != INT4OID ||
			PQftype(res, 5) != INT4OID ||
			PQftype(res, 6) != INT4OID ||
			PQftype(res, 7) != INT4OID)
		{
			pg_fatal("unexpected data types in result set while fetching remote files: %u %u %u",
					 PQftype(res, 0), PQftype(res, 1), PQftype(res, 2));
		}

		if (PQfformat(res, 0) != 1 &&
			PQfformat(res, 1) != 1 &&
			PQfformat(res, 2) != 1 &&
			PQfformat(res, 3) != 1 &&
			PQfformat(res, 4) != 1 &&
			PQfformat(res, 5) != 1 &&
			PQfformat(res, 6) != 1 &&
			PQfformat(res, 7) != 1)
		{
			pg_fatal("unexpected result format while fetching remote files");
		}

		if (PQgetisnull(res, 0, 0) ||
			PQgetisnull(res, 0, 1) ||
			PQgetisnull(res, 0, 3) ||
			PQgetisnull(res, 0, 4) ||
			PQgetisnull(res, 0, 5) ||
			PQgetisnull(res, 0, 6) ||
			PQgetisnull(res, 0, 7))
		{
			pg_fatal("unexpected null values in result while fetching remote files");
		}

		if (PQgetlength(res, 0, 1) != sizeof(int64) ||
			PQgetlength(res, 0, 3) != sizeof(bool) ||
			PQgetlength(res, 0, 4) != sizeof(int32) ||
			PQgetlength(res, 0, 5) != sizeof(int32) ||
			PQgetlength(res, 0, 6) != sizeof(int32) ||
			PQgetlength(res, 0, 7) != sizeof(int32))
		{
			pg_fatal("unexpected result length while fetching remote files");
		}

		/* Read result set to local variables */
		memcpy(&chunkoff, PQgetvalue(res, 0, 1), sizeof(int64));
		chunkoff = pg_ntoh64(chunkoff);
		chunksize = PQgetlength(res, 0, 2);

		filenamelen = PQgetlength(res, 0, 0);
		filename = pg_malloc(filenamelen + 1);
		memcpy(filename, PQgetvalue(res, 0, 0), filenamelen);
		filename[filenamelen] = '\0';

		chunk = PQgetvalue(res, 0, 2);

		memcpy(&iscompressedchunk, PQgetvalue(res, 0, 3), sizeof(bool));
		iscompressedchunk = (iscompressedchunk != 0 ? true : false);

		/*
		 * If a file has been deleted on the source, remove it on the target
		 * as well.  Note that multiple unlink() calls may happen on the same
		 * file if multiple data chunks are associated with it, hence ignore
		 * unconditionally anything missing.  If this file is not a relation
		 * data file, then it has been already truncated when creating the
		 * file chunk list at the previous execution of the filemap.
		 * 
		 * For compressed relation, directly deleting the target file may cause
		 * errors in the subsequent processing of the tuples corresponding to 
		 * the same file, so skip it simply here. This relation can be deleted
		 * when WAL is applied later.
		 */
		if (PQgetisnull(res, 0, 2))
		{
			pg_log_debug("received null value for chunk for file \"%s\", file has been deleted",
						 filename);

			if(!iscompressedchunk)
				remove_target_file(filename, true);
			pg_free(filename);
			PQclear(res);
			continue;
		}

		if(iscompressedchunk)
		{
			int		blocknum, chunknum, nchunks, prealloc_chunks;

			memcpy(&blocknum, PQgetvalue(res, 0, 4), sizeof(int32));
			blocknum = pg_ntoh32(blocknum);

			memcpy(&chunknum, PQgetvalue(res, 0, 5), sizeof(int32));
			chunknum = pg_ntoh32(chunknum);

			memcpy(&nchunks, PQgetvalue(res, 0, 6), sizeof(int32));
			nchunks = pg_ntoh32(nchunks);

			memcpy(&prealloc_chunks, PQgetvalue(res, 0, 7), sizeof(int32));
			prealloc_chunks = pg_ntoh32(prealloc_chunks);

			pg_log_debug("received compressed chunk for file \"%s\", offset %lld, size %d,"
						 " blocknum %d, chunknum %d, nchunks %d, prealloc_chunks %d",
						filename, (long long int) chunkoff, chunksize,
						blocknum, chunknum, nchunks, prealloc_chunks);

			open_target_compressed_relation(filename);

			write_target_compressed_relation_chunk(chunk, chunksize, blocknum, chunknum, nchunks, prealloc_chunks);
		}
		else
		{
			pg_log_debug("received chunk for file \"%s\", offset %lld, size %d",
						filename, (long long int) chunkoff, chunksize);

			open_target_file(filename, false);

			write_target_range(chunk, chunkoff, chunksize);
		}

		pg_free(filename);

		PQclear(res);
	}
	close_target_file();

}

/*
 * Receive a single file as a malloc'd buffer.
 */
char *
libpqGetFile(const char *filename, size_t *filesize)
{
	PGresult   *res;
	char	   *result;
	int			len;
	const char *paramValues[1];

	paramValues[0] = filename;
	res = PQexecParams(conn, "SELECT pg_read_binary_file($1)",
					   1, NULL, paramValues, NULL, NULL, 1);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		pg_fatal("could not fetch remote file \"%s\": %s",
				 filename, PQresultErrorMessage(res));

	/* sanity check the result set */
	if (PQntuples(res) != 1 || PQgetisnull(res, 0, 0))
		pg_fatal("unexpected result set while fetching remote file \"%s\"",
				 filename);

	/* Read result to local variables */
	len = PQgetlength(res, 0, 0);
	result = pg_malloc(len + 1);
	memcpy(result, PQgetvalue(res, 0, 0), len);
	result[len] = '\0';

	PQclear(res);

	pg_log_debug("fetched file \"%s\", length %d", filename, len);

	if (filesize)
		*filesize = len;
	return result;
}

/*
 * Write a file range to a temporary table in the server.
 *
 * The range is sent to the server as a COPY formatted line, to be inserted
 * into the 'fetchchunks' temporary table. It is used in receiveFileChunks()
 * function to actually fetch the data.
 */
static void
fetch_file_range(const char *path, uint64 begin, uint64 end)
{
	char		linebuf[MAXPGPATH + 23];

	/* Split the range into CHUNKSIZE chunks */
	while (end - begin > 0)
	{
		unsigned int len;

		/* Fine as long as CHUNKSIZE is not bigger than UINT32_MAX */
		if (end - begin > CHUNKSIZE)
			len = CHUNKSIZE;
		else
			len = (unsigned int) (end - begin);

		snprintf(linebuf, sizeof(linebuf), "%s\t" UINT64_FORMAT "\t%u\tfalse\t0\t0\t0\t0\n", path, begin, len);

		if (PQputCopyData(conn, linebuf, strlen(linebuf)) != 1)
			pg_fatal("could not send COPY data: %s",
					 PQerrorMessage(conn));

		begin += len;
	}
}

/*
 * Write a compressed relation block range to a temporary table in the server.
 *
 * The range is sent to the server as a COPY formatted line, to be inserted
 * into the 'fetchchunks' temporary table. It is used in receiveFileChunks()
 * function to actually fetch the data.
 */
static void
fetch_compressed_relation_range(const char *path, int chunk_size, BlockNumber blocknum, int nblocks,
								int prealloc_chunks, compressedpagemap_t *first_compressedpagemap)
{
	char		linebuf[MAXPGPATH + 23];
	int			i;
	compressedpagemap_t *compressedpagemap = first_compressedpagemap;

	/* read blocks */
	for(i=0; i < nblocks; i++)
	{
		int			j;
		BlockNumber	blkno = blocknum + i;

		while(compressedpagemap != NULL)
		{
			if(compressedpagemap->blkno == blkno)
				break;
			compressedpagemap = compressedpagemap->next;
		}

		if(compressedpagemap == NULL)
			pg_fatal("could not find compressedpagemap for block %d of file \"%s\"",
					 blkno, path);

		for(j=0; j < compressedpagemap->nchunks; j++)
		{
			uint64		begin;
			int			length = chunk_size;
			pc_chunk_number_t	chunkno = compressedpagemap->chunknos[j];

			begin = OffsetOfPageCompressChunk(chunk_size, chunkno);

			while(j + 1 < compressedpagemap->nchunks && 
			   compressedpagemap->chunknos[j + 1]== compressedpagemap->chunknos[j] + 1)
			{
				length += chunk_size;
				j++;
			}

			snprintf(linebuf, sizeof(linebuf), "%s_pcd\t" UINT64_FORMAT "\t%u\ttrue\t%u\t%u\t%u\t%u\n",
					 path, begin, length, blkno, chunkno,
					 compressedpagemap->nchunks, prealloc_chunks);

			if (PQputCopyData(conn, linebuf, strlen(linebuf)) != 1)
				pg_fatal("could not send COPY data: %s",
						PQerrorMessage(conn));
		}
	}
}

/*
 * Fetch all address of changed compressed blocks from remote source data directory.
 */
void
libpq_fetchCompressedRelationAddress(filemap_t *map)
{
	char		linebuf[MAXPGPATH + 23];
	const char *sql;
	PGresult   *res;
	int			i;

	/*
	 * First create a temporary table, and load it with the blocks that we
	 * need to fetch.
	 */
	sql = "CREATE TEMPORARY TABLE fetchchunks_pca(path text, begin int8, len int4)";
	run_simple_command(sql);

	sql = "COPY fetchchunks_pca FROM STDIN";
	res = PQexec(conn, sql);

	if (PQresultStatus(res) != PGRES_COPY_IN)
		pg_fatal("could not send file list: %s",
				 PQresultErrorMessage(res));
	PQclear(res);

	for (i = 0; i < map->narray; i++)
	{
		file_entry_t *entry;
		int			chunk_size, len;
		datapagemap_iterator_t *iter;
		BlockNumber blkno;
		off_t		range_start = MAX_PAGE_COMPRESS_ADDRESS_FILE_SIZE;
		off_t		range_end = 0;

		entry = map->array[i];

		if(entry->type != FILE_TYPE_COMPRESSED_REL)
			continue;

		chunk_size = entry->chunk_size;
		/* add changed blocks in pagemap */
		iter = datapagemap_iterate(&entry->pagemap);
		while (datapagemap_next(iter, &blkno))
		{
			off_t start, end;

			/* calculate postion of this block in compressed relation address file */

			start = OffsetOfPageCompressAddr(chunk_size, blkno);
			if(start < range_start)
				range_start = start;

			end = OffsetOfPageCompressAddr(chunk_size, blkno + 1);
			if(end > range_end)
				range_end = end;
		}
		pg_free(iter);

		/* add blocks range in source's tail */
		if(entry->action == FILE_ACTION_COPY_TAIL)
		{
			off_t start, end;
		
			/* calculate postion of tail range in compressed relation address file */

			//TODO blkno = (BlockNumber)(entry->oldsize / BLCKSZ);
			start = OffsetOfPageCompressAddr(chunk_size, entry->oldblocks - 1);
			if(start < range_start)
				range_start = start;

			//blkno = (BlockNumber)(entry->newsize / BLCKSZ);
			end = OffsetOfPageCompressAddr(chunk_size, entry->newblocks);
			if(end > range_end)
				range_end = end;
		}

		len = range_end - range_start;

		if(len > 0)
		{
			snprintf(linebuf, sizeof(linebuf), "%s\t" UINT64_FORMAT "\t%u\n", entry->pca->path, range_start, len);

			if (PQputCopyData(conn, linebuf, strlen(linebuf)) != 1)
				pg_fatal("could not send COPY data: %s",
						PQerrorMessage(conn));
		}
	}

	if (PQputCopyEnd(conn, NULL) != 1)
		pg_fatal("could not send end-of-COPY: %s",
				 PQerrorMessage(conn));

	while ((res = PQgetResult(conn)) != NULL)
	{
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
			pg_fatal("unexpected result while sending file list: %s",
					 PQresultErrorMessage(res));
		PQclear(res);
	}

	/*
	 * We've now copied the list of file ranges that we need to fetch to the
	 * temporary table. Now, actually fetch all of those ranges.
	 */
	sql =
		"SELECT path, begin,\n"
		"  pg_read_binary_file(path, begin, len, true) AS chunk\n"
		"FROM fetchchunks_pca\n";

	receiveCompressedRelationAddressFileChunks(map, sql);
}

/*
 * Fetch all changed blocks from remote source data directory.
 */
void
libpq_executeFileMap(filemap_t *map)
{
	file_entry_t *entry;
	const char *sql;
	PGresult   *res;
	int			i;

	/*
	 * First create a temporary table, and load it with the blocks that we
	 * need to fetch.
	 */
	sql = "CREATE TEMPORARY TABLE fetchchunks(path text, begin int8, len int4,"
	    			" iscompressedchunk bool, blocknum int4, chunknum int4,"
					" nchunks int4, prealloc_chunks int4);";
	run_simple_command(sql);

	sql = "COPY fetchchunks FROM STDIN";
	res = PQexec(conn, sql);

	if (PQresultStatus(res) != PGRES_COPY_IN)
		pg_fatal("could not send file list: %s",
				 PQresultErrorMessage(res));
	PQclear(res);

	for (i = 0; i < map->narray; i++)
	{
		bool iscompressedrel = false;
		entry = map->array[i];

		if(entry->type == FILE_TYPE_COMPRESSED_REL)
			iscompressedrel = true;

		/* If this is a relation file, copy the modified blocks */
		execute_pagemap(&entry->pagemap, entry->path, iscompressedrel, entry->chunk_size,
						entry->prealloc_chunks, entry->first_compressedpagemap);

		switch (entry->action)
		{
			case FILE_ACTION_NONE:
				/* nothing else to do */
				break;

			case FILE_ACTION_COPY:
				/* Truncate the old file out of the way, if any */
				open_target_file(entry->path, true);
				fetch_file_range(entry->path, 0, entry->newsize);
				break;

			case FILE_ACTION_TRUNCATE:
				if(iscompressedrel)
					truncate_target_compressed_relation(entry->path, entry->newblocks);
				else
					truncate_target_file(entry->path, entry->newsize);
				break;

			case FILE_ACTION_COPY_TAIL:
				if(iscompressedrel)
					fetch_compressed_relation_range(entry->path, entry->chunk_size, entry->oldblocks,
													entry->newblocks - entry->oldblocks,
													entry->prealloc_chunks,
													entry->first_compressedpagemap);
				else
					fetch_file_range(entry->path, entry->oldsize, entry->newsize);
				break;

			case FILE_ACTION_REMOVE:
				remove_target(entry);
				break;

			case FILE_ACTION_CREATE:
				create_target(entry);
				break;
		}
	}

	if (PQputCopyEnd(conn, NULL) != 1)
		pg_fatal("could not send end-of-COPY: %s",
				 PQerrorMessage(conn));

	while ((res = PQgetResult(conn)) != NULL)
	{
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
			pg_fatal("unexpected result while sending file list: %s",
					 PQresultErrorMessage(res));
		PQclear(res);
	}

	/*
	 * We've now copied the list of file ranges that we need to fetch to the
	 * temporary table. Now, actually fetch all of those ranges.
	 */
	sql =
		"SELECT path, begin,\n"
		"  pg_read_binary_file(path, begin, len, true) AS chunk,\n"
		"  iscompressedchunk, blocknum, chunknum, nchunks, prealloc_chunks\n"
		"FROM fetchchunks\n";

	receiveFileChunks(sql);
}

static void
execute_pagemap(datapagemap_t *pagemap, const char *path, bool iscompressedrel, int chunk_size,
				int prealloc_chunks, compressedpagemap_t *first_compressedpagemap)
{
	datapagemap_iterator_t *iter;
	BlockNumber blkno;
	off_t		offset;

	iter = datapagemap_iterate(pagemap);
	while (datapagemap_next(iter, &blkno))
	{
		if(iscompressedrel)
			fetch_compressed_relation_range(path, chunk_size, blkno, 1, prealloc_chunks, first_compressedpagemap);
		else
		{
			offset = blkno * BLCKSZ;
			fetch_file_range(path, offset, offset + BLCKSZ);
		}
	}
	pg_free(iter);
}
