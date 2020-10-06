/*-------------------------------------------------------------------------
 *
 * file_ops.c
 *	  Helper functions for operating on files.
 *
 * Most of the functions in this file are helper functions for writing to
 * the target data directory. The functions check the --dry-run flag, and
 * do nothing if it's enabled. You should avoid accessing the target files
 * directly but if you do, make sure you honor the --dry-run mode!
 *
 * Portions Copyright (c) 2013-2020, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "common/file_perm.h"
#include "file_ops.h"
#include "filemap.h"
#include "pg_rewind.h"
#include "storage/page_compression.h"
#include "storage/page_compression_impl.h"

/*
 * Currently open target file.
 */
static int	dstfd = -1;
static char dstpath[MAXPGPATH] = "";
static PageCompressHeader	*dstpcmap = NULL;
static char dstpcapath[MAXPGPATH] = "";
static int	chunk_size = 0;

static void create_target_dir(const char *path);
static void remove_target_dir(const char *path);
static void create_target_symlink(const char *path, const char *link);
static void remove_target_symlink(const char *path);

/*
 * Open a target file for writing. If 'trunc' is true and the file already
 * exists, it will be truncated.
 */
void
open_target_file(const char *path, bool trunc)
{
	int			mode;

	if (dry_run)
		return;

	if (dstfd != -1 && !trunc &&
		strcmp(path, &dstpath[strlen(datadir_target) + 1]) == 0)
		return;					/* already open */

	close_target_file();

	snprintf(dstpath, sizeof(dstpath), "%s/%s", datadir_target, path);

	mode = O_WRONLY | O_CREAT | PG_BINARY;
	if (trunc)
		mode |= O_TRUNC;
	dstfd = open(dstpath, mode, pg_file_create_mode);
	if (dstfd < 0)
		pg_fatal("could not open target file \"%s\": %m",
				 dstpath);
}

/*
 * Close target file, if it's open.
 */
void
close_target_file(void)
{
	if (dstfd == -1)
		return;

	if (close(dstfd) != 0)
		pg_fatal("could not close target file \"%s\": %m",
				 dstpath);

	dstfd = -1;

	if(dstpcmap != NULL)
	{
		if (pc_munmap(dstpcmap) != 0)
			pg_fatal("could not munmap file \"%s\": %m", dstpcapath);

		dstpcmap = NULL;
	}
}

void
write_target_range(char *buf, off_t begin, size_t size)
{
	int			writeleft;
	char	   *p;

	/* update progress report */
	fetch_done += size;
	progress_report(false);

	if (dry_run)
		return;

	if (lseek(dstfd, begin, SEEK_SET) == -1)
		pg_fatal("could not seek in target file \"%s\": %m",
				 dstpath);

	writeleft = size;
	p = buf;
	while (writeleft > 0)
	{
		int			writelen;

		errno = 0;
		writelen = write(dstfd, p, writeleft);
		if (writelen < 0)
		{
			/* if write didn't set errno, assume problem is no disk space */
			if (errno == 0)
				errno = ENOSPC;
			pg_fatal("could not write file \"%s\": %m",
					 dstpath);
		}

		p += writelen;
		writeleft -= writelen;
	}

	/* keep the file open, in case we need to copy more blocks in it */
}


void
remove_target(file_entry_t *entry)
{
	Assert(entry->action == FILE_ACTION_REMOVE);

	switch (entry->type)
	{
		case FILE_TYPE_DIRECTORY:
			remove_target_dir(entry->path);
			break;

		case FILE_TYPE_REGULAR:
			remove_target_file(entry->path, false);
			break;

		case FILE_TYPE_SYMLINK:
			remove_target_symlink(entry->path);
			break;
		case FILE_TYPE_COMPRESSED_REL:
			/* can't happen. */
			pg_fatal("invalid action (REMOVE) for compressed relation file");
			break;
	}
}

void
create_target(file_entry_t *entry)
{
	Assert(entry->action == FILE_ACTION_CREATE);

	switch (entry->type)
	{
		case FILE_TYPE_DIRECTORY:
			create_target_dir(entry->path);
			break;

		case FILE_TYPE_SYMLINK:
			create_target_symlink(entry->path, entry->link_target);
			break;

		case FILE_TYPE_REGULAR:
		case FILE_TYPE_COMPRESSED_REL:
			/* can't happen. Regular files are created with open_target_file. */
			pg_fatal("invalid action (CREATE) for regular file");
			break;
	}
}

/*
 * Remove a file from target data directory.  If missing_ok is true, it
 * is fine for the target file to not exist.
 */
void
remove_target_file(const char *path, bool missing_ok)
{
	char		dstpath[MAXPGPATH];

	if (dry_run)
		return;

	snprintf(dstpath, sizeof(dstpath), "%s/%s", datadir_target, path);
	if (unlink(dstpath) != 0)
	{
		if (errno == ENOENT && missing_ok)
			return;

		pg_fatal("could not remove file \"%s\": %m",
				 dstpath);
	}
}

void
truncate_target_file(const char *path, off_t newsize)
{
	char		dstpath[MAXPGPATH];
	int			fd;

	if (dry_run)
		return;

	snprintf(dstpath, sizeof(dstpath), "%s/%s", datadir_target, path);

	fd = open(dstpath, O_WRONLY, pg_file_create_mode);
	if (fd < 0)
		pg_fatal("could not open file \"%s\" for truncation: %m",
				 dstpath);

	if (ftruncate(fd, newsize) != 0)
		pg_fatal("could not truncate file \"%s\" to %u: %m",
				 dstpath, (unsigned int) newsize);

	close(fd);
}

static void
create_target_dir(const char *path)
{
	char		dstpath[MAXPGPATH];

	if (dry_run)
		return;

	snprintf(dstpath, sizeof(dstpath), "%s/%s", datadir_target, path);
	if (mkdir(dstpath, pg_dir_create_mode) != 0)
		pg_fatal("could not create directory \"%s\": %m",
				 dstpath);
}

static void
remove_target_dir(const char *path)
{
	char		dstpath[MAXPGPATH];

	if (dry_run)
		return;

	snprintf(dstpath, sizeof(dstpath), "%s/%s", datadir_target, path);
	if (rmdir(dstpath) != 0)
		pg_fatal("could not remove directory \"%s\": %m",
				 dstpath);
}

static void
create_target_symlink(const char *path, const char *link)
{
	char		dstpath[MAXPGPATH];

	if (dry_run)
		return;

	snprintf(dstpath, sizeof(dstpath), "%s/%s", datadir_target, path);
	if (symlink(link, dstpath) != 0)
		pg_fatal("could not create symbolic link at \"%s\": %m",
				 dstpath);
}

static void
remove_target_symlink(const char *path)
{
	char		dstpath[MAXPGPATH];

	if (dry_run)
		return;

	snprintf(dstpath, sizeof(dstpath), "%s/%s", datadir_target, path);
	if (unlink(dstpath) != 0)
		pg_fatal("could not remove symbolic link \"%s\": %m",
				 dstpath);
}


/*
 * Read a file into memory. The file to be read is <datadir>/<path>.
 * The file contents are returned in a malloc'd buffer, and *filesize
 * is set to the length of the file.
 *
 * The returned buffer is always zero-terminated; the size of the returned
 * buffer is actually *filesize + 1. That's handy when reading a text file.
 * This function can be used to read binary files as well, you can just
 * ignore the zero-terminator in that case.
 *
 * This function is used to implement the fetchFile function in the "fetch"
 * interface (see fetch.c), but is also called directly.
 */
char *
slurpFile(const char *datadir, const char *path, size_t *filesize)
{
	int			fd;
	char	   *buffer;
	struct stat statbuf;
	char		fullpath[MAXPGPATH];
	int			len;
	int			r;

	snprintf(fullpath, sizeof(fullpath), "%s/%s", datadir, path);

	if ((fd = open(fullpath, O_RDONLY | PG_BINARY, 0)) == -1)
		pg_fatal("could not open file \"%s\" for reading: %m",
				 fullpath);

	if (fstat(fd, &statbuf) < 0)
		pg_fatal("could not open file \"%s\" for reading: %m",
				 fullpath);

	len = statbuf.st_size;

	buffer = pg_malloc(len + 1);

	r = read(fd, buffer, len);
	if (r != len)
	{
		if (r < 0)
			pg_fatal("could not read file \"%s\": %m",
					 fullpath);
		else
			pg_fatal("could not read file \"%s\": read %d of %zu",
					 fullpath, r, (Size) len);
	}
	close(fd);

	/* Zero-terminate the buffer. */
	buffer[len] = '\0';

	if (filesize)
		*filesize = len;
	return buffer;
}


/*
 * Open a target compressed relation for writing. If 'trunc' is true and the
 * file already exists, it will be truncated.
 */
void
open_target_compressed_relation(const char *path)
{
	char		localpath[MAXPGPATH];
	int			mode;
	int			fd, ret;
	PageCompressHeader	pchdr;

	if (dry_run)
		return;

	snprintf(localpath, sizeof(localpath), "%s/%s_pcd", datadir_target, path);

	if (dstfd != -1 &&
		strcmp(localpath, dstpath) == 0)
		return;					/* already open */

	close_target_file();

	/* read page compression file header of target relation */
	snprintf(dstpcapath, sizeof(dstpcapath), "%s/%s_pca", datadir_target, path);

	fd = open(dstpcapath, O_RDWR | PG_BINARY, 0);
	if (fd < 0)
		pg_fatal("could not open target file \"%s\": %m",
				 dstpcapath);

	ret = read(fd, &pchdr, sizeof(PageCompressHeader));
	if(ret != sizeof(PageCompressHeader))
		pg_fatal("could not read compressed page address file \"%s\"",
				 dstpcapath);

	dstpcmap = pc_mmap(fd, pchdr.chunk_size, false);
	if(dstpcmap == MAP_FAILED)
		pg_fatal("Failed to mmap page compression address file %s: %m",
				dstpcapath);

	if (close(fd) != 0)
		pg_fatal("could not close file \"%s\": %m", dstpcapath);

	/* open page compression data file */
	snprintf(dstpath, sizeof(dstpath), "%s/%s_pcd", datadir_target, path);

	mode = O_WRONLY | O_CREAT | PG_BINARY;
	dstfd = open(dstpath, mode, pg_file_create_mode);
	if (dstfd < 0)
		pg_fatal("could not open target file \"%s\": %m",
				 dstpath);

	chunk_size = pchdr.chunk_size;
}


void
truncate_target_compressed_relation(const char *path, int newblocks)
{
	char				dstpath[MAXPGPATH];
	int					fd, ret;
	PageCompressHeader	pchdr, *dstpcmap;

	if (dry_run)
		return;

	/* read page compression file header of target relation */
	snprintf(dstpath, sizeof(dstpath), "%s/%s_pca", datadir_target, path);

	fd = open(dstpath, O_RDWR | PG_BINARY, pg_file_create_mode);
	if (fd < 0)
		pg_fatal("could not open target file \"%s\": %m",
				 dstpath);

	ret = read(fd, &pchdr, sizeof(PageCompressHeader));
	if(ret != sizeof(PageCompressHeader))
		pg_fatal("could not read compressed page address file \"%s\"",
				 dstpath);

	dstpcmap = pc_mmap(fd, pchdr.chunk_size, false);
	if(dstpcmap == MAP_FAILED)
		pg_fatal("Failed to mmap page compression address file %s: %m",
				dstpath);

	if (close(fd) != 0)
		pg_fatal("could not close file \"%s\": %m", dstpcapath);

	/* resize target relation */
	if(dstpcmap->nblocks > newblocks)
	{
		int blk;
		for(blk = newblocks; blk < dstpcmap->nblocks; blk++)
		{
			PageCompressAddr *pcAddr = GetPageCompressAddr(dstpcmap, pchdr.chunk_size, blk);
			pcAddr->nchunks = 0;
		}
		dstpcmap->nblocks = newblocks;
	}

	if (pc_munmap(dstpcmap) != 0)
		pg_fatal("could not munmap file \"%s\": %m", dstpath);
}


void
write_target_compressed_relation_chunk(char *buf, size_t size, int blocknum, int chunknum, int nchunks, int prealloc_chunks)
{
	PageCompressAddr	*pcAddr;
	int					i;
	int		writeleft;
	int		writed = 0;
	char	*p = buf;
	int		seekpos;
	int		allocated_chunks = nchunks;

	/* update progress report */
	fetch_done += size;
	progress_report(false);

	if (dry_run)
		return;

	if(size <= 0)
		return;

	pcAddr = GetPageCompressAddr(dstpcmap, chunk_size, blocknum);

	/* allocate chunks */
	if(allocated_chunks < prealloc_chunks)
		allocated_chunks = prealloc_chunks;

	if(pcAddr->allocated_chunks < allocated_chunks)
	{
		for(i = pcAddr->allocated_chunks; i< allocated_chunks; i++)
		{
			pcAddr->chunknos[i] = dstpcmap->allocated_chunks + 1;
			dstpcmap->allocated_chunks++;
		}
		pcAddr->allocated_chunks = allocated_chunks;
	}

	if(dstpcmap->nblocks <= blocknum)
		dstpcmap->nblocks = blocknum + 1;
	
	if(pcAddr->nchunks != nchunks)
		pcAddr->nchunks = nchunks;

	while(writed < size)
	{
		/* write one chunk to target */

		writeleft = size - writed;
		if(writeleft > chunk_size)
			writeleft = chunk_size;

		seekpos = OffsetOfPageCompressChunk(chunk_size, pcAddr->chunknos[chunknum + writed / chunk_size]);

		if (lseek(dstfd, seekpos, SEEK_SET) == -1)
			pg_fatal("could not seek in target file \"%s\": %m",
						dstpath);

		while (writeleft > 0)
		{
			int			writelen;

			errno = 0;
			writelen = write(dstfd, p, writeleft);
			if (writelen < 0)
			{
				/* if write didn't set errno, assume problem is no disk space */
				if (errno == 0)
					errno = ENOSPC;
				pg_fatal("could not write file \"%s\": %m",
						dstpath);
			}

			p += writelen;
			writeleft -= writelen;
			writed += writelen;
		}
	}
	/* keep the file open, in case we need to copy more blocks in it */
}
