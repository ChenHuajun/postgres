/*-------------------------------------------------------------------------
 *
 * copy_fetch.c
 *	  Functions for using a data directory as the source.
 *
 * Portions Copyright (c) 2013-2020, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <sys/stat.h>
#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>

#include "common/string.h"
#include "datapagemap.h"
#include "fetch.h"
#include "file_ops.h"
#include "filemap.h"
#include "pg_rewind.h"
#include "storage/page_compression.h"

static void recurse_dir(const char *datadir, const char *path,
						process_file_callback_t callback);

static void execute_pagemap(datapagemap_t *pagemap, const char *path,
							bool iscompressedrel, int chunk_size,
							int prealloc_chunks, compressedpagemap_t *first_compressedpagemap);
static void rewind_copy_compressed_relation_range(const char *path, int chunk_size, 
												  BlockNumber blocknum, int nblocks,
												  int prealloc_chunks,
												  compressedpagemap_t *first_compressedpagemap);
/*
 * Traverse through all files in a data directory, calling 'callback'
 * for each file.
 */
void
traverse_datadir(const char *datadir, process_file_callback_t callback)
{
	recurse_dir(datadir, NULL, callback);
}

/*
 * recursive part of traverse_datadir
 *
 * parentpath is the current subdirectory's path relative to datadir,
 * or NULL at the top level.
 */
static void
recurse_dir(const char *datadir, const char *parentpath,
			process_file_callback_t callback)
{
	DIR		   *xldir;
	struct dirent *xlde;
	char		fullparentpath[MAXPGPATH];

	if (parentpath)
		snprintf(fullparentpath, MAXPGPATH, "%s/%s", datadir, parentpath);
	else
		snprintf(fullparentpath, MAXPGPATH, "%s", datadir);

	xldir = opendir(fullparentpath);
	if (xldir == NULL)
		pg_fatal("could not open directory \"%s\": %m",
				 fullparentpath);

	while (errno = 0, (xlde = readdir(xldir)) != NULL)
	{
		struct stat fst;
		char		fullpath[MAXPGPATH * 2];
		char		path[MAXPGPATH * 2];

		if (strcmp(xlde->d_name, ".") == 0 ||
			strcmp(xlde->d_name, "..") == 0)
			continue;

		snprintf(fullpath, sizeof(fullpath), "%s/%s", fullparentpath, xlde->d_name);

		if (lstat(fullpath, &fst) < 0)
		{
			if (errno == ENOENT)
			{
				/*
				 * File doesn't exist anymore. This is ok, if the new primary
				 * is running and the file was just removed. If it was a data
				 * file, there should be a WAL record of the removal. If it
				 * was something else, it couldn't have been anyway.
				 *
				 * TODO: But complain if we're processing the target dir!
				 */
			}
			else
				pg_fatal("could not stat file \"%s\": %m",
						 fullpath);
		}

		if (parentpath)
			snprintf(path, sizeof(path), "%s/%s", parentpath, xlde->d_name);
		else
			snprintf(path, sizeof(path), "%s", xlde->d_name);

		if (S_ISREG(fst.st_mode))
		{
			if(pg_str_endswith(path, "_pca"))
			{			
				int					fd, ret;
				PageCompressHeader	pchdr;

				/* read header of compressed relation address file */
				fd = open(fullpath, O_RDONLY | PG_BINARY, 0);
				if (fd < 0)
					pg_fatal("could not open file \"%s\": %m",
							fullpath);

				ret = read(fd, &pchdr, sizeof(PageCompressHeader));
				if(ret == sizeof(PageCompressHeader))
					callback(path, FILE_TYPE_REGULAR, fst.st_size, NULL, &pchdr);
				else
					callback(path, FILE_TYPE_REGULAR, fst.st_size, NULL, NULL);

				close(fd);
			}
			else
				callback(path, FILE_TYPE_REGULAR, fst.st_size, NULL, NULL);
		}
		else if (S_ISDIR(fst.st_mode))
		{
			callback(path, FILE_TYPE_DIRECTORY, 0, NULL, NULL);
			/* recurse to handle subdirectories */
			recurse_dir(datadir, path, callback);
		}
#ifndef WIN32
		else if (S_ISLNK(fst.st_mode))
#else
		else if (pgwin32_is_junction(fullpath))
#endif
		{
#if defined(HAVE_READLINK) || defined(WIN32)
			char		link_target[MAXPGPATH];
			int			len;

			len = readlink(fullpath, link_target, sizeof(link_target));
			if (len < 0)
				pg_fatal("could not read symbolic link \"%s\": %m",
						 fullpath);
			if (len >= sizeof(link_target))
				pg_fatal("symbolic link \"%s\" target is too long",
						 fullpath);
			link_target[len] = '\0';

			callback(path, FILE_TYPE_SYMLINK, 0, link_target, NULL);

			/*
			 * If it's a symlink within pg_tblspc, we need to recurse into it,
			 * to process all the tablespaces.  We also follow a symlink if
			 * it's for pg_wal.  Symlinks elsewhere are ignored.
			 */
			if ((parentpath && strcmp(parentpath, "pg_tblspc") == 0) ||
				strcmp(path, "pg_wal") == 0)
				recurse_dir(datadir, path, callback);
#else
			pg_fatal("\"%s\" is a symbolic link, but symbolic links are not supported on this platform",
					 fullpath);
#endif							/* HAVE_READLINK */
		}
	}

	if (errno)
		pg_fatal("could not read directory \"%s\": %m",
				 fullparentpath);

	if (closedir(xldir))
		pg_fatal("could not close directory \"%s\": %m",
				 fullparentpath);
}

void
local_fetchCompressedRelationAddress(filemap_t *map)
{
	char		srcpath[MAXPGPATH];
	file_entry_t *entry;
	int			chunk_size;
	int			fd, i;
	PageCompressHeader	*pcMap;

	for (i = 0; i < map->narray; i++)
	{
		entry = map->array[i];

		if(entry->type != FILE_TYPE_COMPRESSED_REL)
			continue;

		chunk_size = entry->chunk_size;

		/* read page compression file header of source relation */
		snprintf(srcpath, sizeof(srcpath), "%s/%s", datadir_source, entry->pca->path);
		fd = open(srcpath, O_RDONLY | PG_BINARY, 0);
		if (fd < 0)
			pg_fatal("could not open source file \"%s\": %m",
					srcpath);

		pcMap = pc_mmap(fd, chunk_size, true);
		if(pcMap == MAP_FAILED)
			pg_fatal("Failed to mmap page compression address file %s: %m",
					srcpath);

		if (close(fd) != 0)
			pg_fatal("could not close file \"%s\": %m", srcpath);
		
		fill_compressed_relation_address(entry, NULL, pcMap);

		if (pc_munmap(pcMap) != 0)
			pg_fatal("could not munmap file \"%s\": %m", srcpath);
	}
}

/*
 * Copy a file from source to target, between 'begin' and 'end' offsets.
 *
 * If 'trunc' is true, any existing file with the same name is truncated.
 */
static void
rewind_copy_file_range(const char *path, off_t begin, off_t end, bool trunc)
{
	PGAlignedBlock buf;
	char		srcpath[MAXPGPATH];
	int			srcfd;

	snprintf(srcpath, sizeof(srcpath), "%s/%s", datadir_source, path);

	srcfd = open(srcpath, O_RDONLY | PG_BINARY, 0);
	if (srcfd < 0)
		pg_fatal("could not open source file \"%s\": %m",
				 srcpath);

	if (lseek(srcfd, begin, SEEK_SET) == -1)
		pg_fatal("could not seek in source file: %m");

	open_target_file(path, trunc);

	while (end - begin > 0)
	{
		int			readlen;
		int			len;

		if (end - begin > sizeof(buf))
			len = sizeof(buf);
		else
			len = end - begin;

		readlen = read(srcfd, buf.data, len);

		if (readlen < 0)
			pg_fatal("could not read file \"%s\": %m",
					 srcpath);
		else if (readlen == 0)
			pg_fatal("unexpected EOF while reading file \"%s\"", srcpath);

		write_target_range(buf.data, begin, readlen);
		begin += readlen;
	}

	if (close(srcfd) != 0)
		pg_fatal("could not close file \"%s\": %m", srcpath);
}

/*
 * Copy a file from source to target, between 'begin' and 'end' offsets.
 *
 * If 'trunc' is true, any existing file with the same name is truncated.
 */
static void 
rewind_copy_compressed_relation_range(const char *path, int chunk_size, 
									  BlockNumber blocknum, int nblocks,
									  int prealloc_chunks,
									  compressedpagemap_t *first_compressedpagemap)
{
	PGAlignedBlock buf;
	char		srcpath[MAXPGPATH];
	int			fd, i;
	compressedpagemap_t *compressedpagemap = first_compressedpagemap;

	/* open source compressed relation data file */
	snprintf(srcpath, sizeof(srcpath), "%s/%s_pcd", datadir_source, path);

	fd = open(srcpath, O_RDONLY | PG_BINARY, 0);
	if (fd < 0)
		pg_fatal("could not open source file \"%s\": %m",
				srcpath);

	/* copy blocks from source to target */
	open_target_compressed_relation(path);

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
			int		readlen;
			int		seekpos;
			int		length = chunk_size;
			pc_chunk_number_t	chunkno = compressedpagemap->chunknos[j];

			seekpos = OffsetOfPageCompressChunk(chunk_size, chunkno);

			while(j + 1 < compressedpagemap->nchunks && 
			   compressedpagemap->chunknos[j + 1]== compressedpagemap->chunknos[j] + 1)
			{
				length += chunk_size;
				j++;
			}

			if (lseek(fd, seekpos, SEEK_SET) == -1)
				pg_fatal("could not seek in source file: %m");

			readlen = read(fd, buf.data, length);
			if (readlen < 0)
				pg_fatal("could not read file \"%s\": %m",
						srcpath);
			else if (readlen == 0)
				pg_fatal("unexpected EOF while reading file \"%s\"", srcpath);

			write_target_compressed_relation_chunk(buf.data, readlen, blkno, chunkno,
												   compressedpagemap->nchunks, prealloc_chunks);
		}
	}

	if (close(fd) != 0)
		pg_fatal("could not close file \"%s\": %m", srcpath);
}


/*
 * Copy all relation data files from datadir_source to datadir_target, which
 * are marked in the given data page map.
 */
void
copy_executeFileMap(filemap_t *map)
{
	file_entry_t *entry;
	int			i;

	for (i = 0; i < map->narray; i++)
	{
		bool	iscompressedrel = false;

		entry = map->array[i];

		if(entry->type == FILE_TYPE_COMPRESSED_REL)
			iscompressedrel = true;

		execute_pagemap(&entry->pagemap, entry->path, iscompressedrel, entry->chunk_size,
					    entry->prealloc_chunks, entry->first_compressedpagemap);

		switch (entry->action)
		{
			case FILE_ACTION_NONE:
				/* ok, do nothing.. */
				break;

			case FILE_ACTION_COPY:
				rewind_copy_file_range(entry->path, 0, entry->newsize, true);
				break;

			case FILE_ACTION_TRUNCATE:
				if(iscompressedrel)
					truncate_target_compressed_relation(entry->path, entry->newblocks);
				else
					truncate_target_file(entry->path, entry->newsize);
				break;

			case FILE_ACTION_COPY_TAIL:
				if(iscompressedrel)
					rewind_copy_compressed_relation_range(entry->path,
														  entry->chunk_size,
														  entry->oldblocks,
														  entry->newblocks - entry->oldblocks,
														  entry->prealloc_chunks,
														  entry->first_compressedpagemap);
				else
					rewind_copy_file_range(entry->path, entry->oldsize,
										entry->newsize, false);
				break;

			case FILE_ACTION_CREATE:
				create_target(entry);
				break;

			case FILE_ACTION_REMOVE:
				remove_target(entry);
				break;
		}
	}

	close_target_file();
}

static void
execute_pagemap(datapagemap_t *pagemap, const char *path,
				bool iscompressedrel, int chunk_size,
				int prealloc_chunks, compressedpagemap_t *first_compressedpagemap)
{
	datapagemap_iterator_t *iter;
	BlockNumber blkno;
	off_t		offset;

	iter = datapagemap_iterate(pagemap);
	while (datapagemap_next(iter, &blkno))
	{
		if(iscompressedrel)
			rewind_copy_compressed_relation_range(path, chunk_size, blkno, 1,
												  prealloc_chunks, first_compressedpagemap);
		else
		{
			offset = blkno * BLCKSZ;
			rewind_copy_file_range(path, offset, offset + BLCKSZ, false);
		}
		/* Ok, this block has now been copied from new data dir to old */
	}
	pg_free(iter);
}
