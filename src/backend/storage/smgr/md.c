/*-------------------------------------------------------------------------
 *
 * md.c
 *	  This code manages relations that reside on magnetic disk.
 *
 * Or at least, that was what the Berkeley folk had in mind when they named
 * this file.  In reality, what this code provides is an interface from
 * the smgr API to Unix-like filesystem APIs, so it will work with any type
 * of device for which the operating system provides filesystem support.
 * It doesn't matter whether the bits are on spinning rust or some other
 * storage technology.
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/smgr/md.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/mman.h>

#include "access/xlog.h"
#include "access/xlogutils.h"
#include "commands/tablespace.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "pgstat.h"
#include "postmaster/bgwriter.h"
#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "storage/md.h"
#include "storage/relfilenode.h"
#include "storage/smgr.h"
#include "storage/sync.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "storage/page_compression.h"

/*
 *	The magnetic disk storage manager keeps track of open file
 *	descriptors in its own descriptor pool.  This is done to make it
 *	easier to support relations that are larger than the operating
 *	system's file size limit (often 2GBytes).  In order to do that,
 *	we break relations up into "segment" files that are each shorter than
 *	the OS file size limit.  The segment size is set by the RELSEG_SIZE
 *	configuration constant in pg_config.h.
 *
 *	On disk, a relation must consist of consecutively numbered segment
 *	files in the pattern
 *		-- Zero or more full segments of exactly RELSEG_SIZE blocks each
 *		-- Exactly one partial segment of size 0 <= size < RELSEG_SIZE blocks
 *		-- Optionally, any number of inactive segments of size 0 blocks.
 *	The full and partial segments are collectively the "active" segments.
 *	Inactive segments are those that once contained data but are currently
 *	not needed because of an mdtruncate() operation.  The reason for leaving
 *	them present at size zero, rather than unlinking them, is that other
 *	backends and/or the checkpointer might be holding open file references to
 *	such segments.  If the relation expands again after mdtruncate(), such
 *	that a deactivated segment becomes active again, it is important that
 *	such file references still be valid --- else data might get written
 *	out to an unlinked old copy of a segment file that will eventually
 *	disappear.
 *
 *	File descriptors are stored in the per-fork md_seg_fds arrays inside
 *	SMgrRelation. The length of these arrays is stored in md_num_open_segs.
 *	Note that a fork's md_num_open_segs having a specific value does not
 *	necessarily mean the relation doesn't have additional segments; we may
 *	just not have opened the next segment yet.  (We could not have "all
 *	segments are in the array" as an invariant anyway, since another backend
 *	could extend the relation while we aren't looking.)  We do not have
 *	entries for inactive segments, however; as soon as we find a partial
 *	segment, we assume that any subsequent segments are inactive.
 *
 *	The entire MdfdVec array is palloc'd in the MdCxt memory context.
 */

typedef struct _MdfdVec
{
	File		mdfd_vfd;		/* fd number in fd.c's pool */
	File		mdfd_vfd_pca;	/* page compression address file 's fd number in fd.c's pool */
	File		mdfd_vfd_pcd;	/* page compression data file 's fd number in fd.c's pool */
	BlockNumber mdfd_segno;		/* segment number, from 0 */
} MdfdVec;

static MemoryContext MdCxt;		/* context for all MdfdVec objects */


/* Populate a file tag describing an md.c segment file. */
#define INIT_MD_FILETAG(a,xx_rnode,xx_forknum,xx_segno) \
( \
	memset(&(a), 0, sizeof(FileTag)), \
	(a).handler = SYNC_HANDLER_MD, \
	(a).rnode = (xx_rnode), \
	(a).forknum = (xx_forknum), \
	(a).segno = (xx_segno) \
)


/*** behavior for mdopen & _mdfd_getseg ***/
/* ereport if segment not present */
#define EXTENSION_FAIL				(1 << 0)
/* return NULL if segment not present */
#define EXTENSION_RETURN_NULL		(1 << 1)
/* create new segments as needed */
#define EXTENSION_CREATE			(1 << 2)
/* create new segments if needed during recovery */
#define EXTENSION_CREATE_RECOVERY	(1 << 3)
/*
 * Allow opening segments which are preceded by segments smaller than
 * RELSEG_SIZE, e.g. inactive segments (see above). Note that this breaks
 * mdnblocks() and related functionality henceforth - which currently is ok,
 * because this is only required in the checkpointer which never uses
 * mdnblocks().
 */
#define EXTENSION_DONT_CHECK_SIZE	(1 << 4)

#define IS_COMPRESSED_MAINFORK(reln, forkNum) \
	(reln->smgr_rnode.node.compress_algorithm != COMPRESS_TYPE_NONE && forkNum == MAIN_FORKNUM)

#define PAGE_COMPRESS_ALGORITHM(reln) (reln->smgr_rnode.node.compress_algorithm)
#define PAGE_COMPRESS_LEVEL(reln) (reln->smgr_rnode.node.compress_level)
#define PAGE_COMPRESS_CHUNK_SIZE(reln) (reln->smgr_rnode.node.compress_chunk_size)
#define PAGE_COMPRESS_PREALLOC_CHUNKS(reln) (reln->smgr_rnode.node.compress_prealloc_chunks)

/* local routines */
static void mdunlinkfork(RelFileNodeBackend rnode, ForkNumber forkNum,
						 bool isRedo);
static MdfdVec *mdopenfork(SMgrRelation reln, ForkNumber forknum, int behavior);
static void register_dirty_segment(SMgrRelation reln, ForkNumber forknum,
								   MdfdVec *seg);
static void register_unlink_segment(RelFileNodeBackend rnode, ForkNumber forknum,
									BlockNumber segno);
static void register_forget_request(RelFileNodeBackend rnode, ForkNumber forknum,
									BlockNumber segno);
static void _fdvec_resize(SMgrRelation reln,
						  ForkNumber forknum,
						  int nseg);
static char *_mdfd_segpath(SMgrRelation reln, ForkNumber forknum,
						   BlockNumber segno);
static MdfdVec *_mdfd_openseg(SMgrRelation reln, ForkNumber forkno,
							  BlockNumber segno, int oflags);
static MdfdVec *_mdfd_getseg(SMgrRelation reln, ForkNumber forkno,
							 BlockNumber blkno, bool skipFsync, int behavior);
static BlockNumber _mdnblocks(SMgrRelation reln, ForkNumber forknum,
							  MdfdVec *seg);

static int sync_pcmap(PageCompressHeader *pcMap, uint32 wait_event_info);

/*
 *	mdinit() -- Initialize private state for magnetic disk storage manager.
 */
void
mdinit(void)
{
	MdCxt = AllocSetContextCreate(TopMemoryContext,
								  "MdSmgr",
								  ALLOCSET_DEFAULT_SIZES);
}

/*
 *	mdexists() -- Does the physical file exist?
 *
 * Note: this will return true for lingering files, with pending deletions
 */
bool
mdexists(SMgrRelation reln, ForkNumber forkNum)
{
	/*
	 * Close it first, to ensure that we notice if the fork has been unlinked
	 * since we opened it.
	 */
	mdclose(reln, forkNum);

	return (mdopenfork(reln, forkNum, EXTENSION_RETURN_NULL) != NULL);
}

/*
 *	mdcreate() -- Create a new relation on magnetic disk.
 *
 * If isRedo is true, it's okay for the relation to exist already.
 */
void
mdcreate(SMgrRelation reln, ForkNumber forkNum, bool isRedo)
{
	MdfdVec    *mdfd;
	char	   *path;
	char	   *pcfile_path;
	File		fd,fd_pca,fd_pcd;

	if (isRedo && reln->md_num_open_segs[forkNum] > 0)
		return;					/* created and opened already... */

	Assert(reln->md_num_open_segs[forkNum] == 0);

	/*
	 * We may be using the target table space for the first time in this
	 * database, so create a per-database subdirectory if needed.
	 *
	 * XXX this is a fairly ugly violation of module layering, but this seems
	 * to be the best place to put the check.  Maybe TablespaceCreateDbspace
	 * should be here and not in commands/tablespace.c?  But that would imply
	 * importing a lot of stuff that smgr.c oughtn't know, either.
	 */
	TablespaceCreateDbspace(reln->smgr_rnode.node.spcNode,
							reln->smgr_rnode.node.dbNode,
							isRedo);

	path = relpath(reln->smgr_rnode, forkNum);

	fd = PathNameOpenFile(path, O_RDWR | O_CREAT | O_EXCL | PG_BINARY);

	if (fd < 0)
	{
		int			save_errno = errno;

		if (isRedo)
			fd = PathNameOpenFile(path, O_RDWR | PG_BINARY);
		if (fd < 0)
		{
			/* be sure to report the error reported by create, not open */
			errno = save_errno;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not create file \"%s\": %m", path)));
		}
	}

	fd_pca = -1;
	fd_pcd = -1;
	if(IS_COMPRESSED_MAINFORK(reln,forkNum))
	{
		/* close main fork file */
		FileClose(fd);
		fd = -1;

		/* open page compress address file */
		pcfile_path = psprintf("%s_pca", path);
		fd_pca = PathNameOpenFile(pcfile_path, O_RDWR | O_CREAT | O_EXCL | PG_BINARY);

		if (fd_pca < 0)
		{
			int			save_errno = errno;

			if (isRedo)
				fd_pca = PathNameOpenFile(pcfile_path, O_RDWR | PG_BINARY);
			if (fd_pca < 0)
			{
				/* be sure to report the error reported by create, not open */
				errno = save_errno;
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not create file \"%s\": %m", pcfile_path)));
			}
		}
		pfree(pcfile_path);

		/* open page compress data file */
		pcfile_path = psprintf("%s_pcd", path);
		fd_pcd = PathNameOpenFile(pcfile_path, O_RDWR | O_CREAT | O_EXCL | PG_BINARY);

		if (fd_pcd < 0)
		{
			int			save_errno = errno;

			if (isRedo)
				fd_pcd = PathNameOpenFile(pcfile_path, O_RDWR | PG_BINARY);
			if (fd_pcd < 0)
			{
				/* be sure to report the error reported by create, not open */
				errno = save_errno;
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not create file \"%s\": %m", pcfile_path)));
			}
		}
		pfree(pcfile_path);

		SetupPageCompressMemoryMap(fd_pca, PAGE_COMPRESS_CHUNK_SIZE(reln), PAGE_COMPRESS_ALGORITHM(reln));
	}

	pfree(path);

	_fdvec_resize(reln, forkNum, 1);
	mdfd = &reln->md_seg_fds[forkNum][0];
	mdfd->mdfd_vfd = fd;
	mdfd->mdfd_vfd_pca = fd_pca;
	mdfd->mdfd_vfd_pcd = fd_pcd;
	mdfd->mdfd_segno = 0;
}

/*
 *	mdunlink() -- Unlink a relation.
 *
 * Note that we're passed a RelFileNodeBackend --- by the time this is called,
 * there won't be an SMgrRelation hashtable entry anymore.
 *
 * forkNum can be a fork number to delete a specific fork, or InvalidForkNumber
 * to delete all forks.
 *
 * For regular relations, we don't unlink the first segment file of the rel,
 * but just truncate it to zero length, and record a request to unlink it after
 * the next checkpoint.  Additional segments can be unlinked immediately,
 * however.  Leaving the empty file in place prevents that relfilenode
 * number from being reused.  The scenario this protects us from is:
 * 1. We delete a relation (and commit, and actually remove its file).
 * 2. We create a new relation, which by chance gets the same relfilenode as
 *	  the just-deleted one (OIDs must've wrapped around for that to happen).
 * 3. We crash before another checkpoint occurs.
 * During replay, we would delete the file and then recreate it, which is fine
 * if the contents of the file were repopulated by subsequent WAL entries.
 * But if we didn't WAL-log insertions, but instead relied on fsyncing the
 * file after populating it (as we do at wal_level=minimal), the contents of
 * the file would be lost forever.  By leaving the empty file until after the
 * next checkpoint, we prevent reassignment of the relfilenode number until
 * it's safe, because relfilenode assignment skips over any existing file.
 *
 * We do not need to go through this dance for temp relations, though, because
 * we never make WAL entries for temp rels, and so a temp rel poses no threat
 * to the health of a regular rel that has taken over its relfilenode number.
 * The fact that temp rels and regular rels have different file naming
 * patterns provides additional safety.
 *
 * All the above applies only to the relation's main fork; other forks can
 * just be removed immediately, since they are not needed to prevent the
 * relfilenode number from being recycled.  Also, we do not carefully
 * track whether other forks have been created or not, but just attempt to
 * unlink them unconditionally; so we should never complain about ENOENT.
 *
 * If isRedo is true, it's unsurprising for the relation to be already gone.
 * Also, we should remove the file immediately instead of queuing a request
 * for later, since during redo there's no possibility of creating a
 * conflicting relation.
 *
 * Note: any failure should be reported as WARNING not ERROR, because
 * we are usually not in a transaction anymore when this is called.
 */
void
mdunlink(RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo)
{
	/* Now do the per-fork work */
	if (forkNum == InvalidForkNumber)
	{
		for (forkNum = 0; forkNum <= MAX_FORKNUM; forkNum++)
			mdunlinkfork(rnode, forkNum, isRedo);
	}
	else
		mdunlinkfork(rnode, forkNum, isRedo);
}

static void
mdunlinkfork(RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo)
{
	char	   *path;
	int			ret;

	path = relpath(rnode, forkNum);

	/*
	 * Delete or truncate the first segment.
	 */
	if (isRedo || forkNum != MAIN_FORKNUM || RelFileNodeBackendIsTemp(rnode))
	{
		/* First, forget any pending sync requests for the first segment */
		if (!RelFileNodeBackendIsTemp(rnode))
			register_forget_request(rnode, forkNum, 0 /* first seg */ );

		/* Next unlink the file */
		ret = unlink(path);
		if (ret < 0 && errno != ENOENT)
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("could not remove file \"%s\": %m", path)));

		if(rnode.node.compress_algorithm != COMPRESS_TYPE_NONE &&
			forkNum == MAIN_FORKNUM)
		{
			char *pcfile_path;

			pcfile_path = psprintf("%s_pca", path);
			ret = unlink(pcfile_path);
			if (ret < 0 && errno != ENOENT)
				ereport(WARNING,
						(errcode_for_file_access(),
						errmsg("could not remove file \"%s\": %m", pcfile_path)));
			pfree(pcfile_path);

			pcfile_path = psprintf("%s_pcd", path);
			ret = unlink(pcfile_path);
			if (ret < 0 && errno != ENOENT)
				ereport(WARNING,
						(errcode_for_file_access(),
						errmsg("could not remove file \"%s\": %m", pcfile_path)));
			pfree(pcfile_path);
		}
	}
	else
	{
		/* truncate(2) would be easier here, but Windows hasn't got it */
		int			fd;

		fd = OpenTransientFile(path, O_RDWR | PG_BINARY);
		if (fd >= 0)
		{
			int			save_errno;

			ret = ftruncate(fd, 0);
			save_errno = errno;
			CloseTransientFile(fd);
			errno = save_errno;
		}
		else
			ret = -1;
		if (ret < 0 && errno != ENOENT)
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("could not truncate file \"%s\": %m", path)));

		if(rnode.node.compress_algorithm != COMPRESS_TYPE_NONE &&
			forkNum == MAIN_FORKNUM)
		{
			char				*pcfile_path;

			/* clear page compression address file */
			pcfile_path = psprintf("%s_pca", path);
			fd = OpenTransientFile(pcfile_path, O_RDWR | PG_BINARY);
			if (fd >= 0)
			{
				int		save_errno;

				ret = ftruncate(fd, 0);
				save_errno = errno;
				CloseTransientFile(fd);
				errno = save_errno;
			}
			else
				ret = -1;
			if (ret < 0 && errno != ENOENT)
				ereport(WARNING,
						(errcode_for_file_access(),
						errmsg("could not truncate file \"%s\": %m", pcfile_path)));
			pfree(pcfile_path);

			/* truncate page compression data file */
			pcfile_path = psprintf("%s_pcd", path);
			fd = OpenTransientFile(pcfile_path, O_RDWR | PG_BINARY);
			if (fd >= 0)
			{
				int		save_errno;

				ret = ftruncate(fd, 0);
				save_errno = errno;
				CloseTransientFile(fd);
				errno = save_errno;
			}
			else
				ret = -1;
			if (ret < 0 && errno != ENOENT)
				ereport(WARNING,
						(errcode_for_file_access(),
						errmsg("could not truncate file \"%s\": %m", path)));
			pfree(pcfile_path);
		}

		/* Register request to unlink first segment later */
		register_unlink_segment(rnode, forkNum, 0 /* first seg */ );
	}

	/*
	 * Delete any additional segments.
	 */
	if (ret >= 0)
	{
		char	   *segpath = (char *) palloc(strlen(path) + 12);
		BlockNumber segno;

		/*
		 * Note that because we loop until getting ENOENT, we will correctly
		 * remove all inactive segments as well as active ones.
		 */
		for (segno = 1;; segno++)
		{
			/*
			 * Forget any pending sync requests for this segment before we try
			 * to unlink.
			 */
			if (!RelFileNodeBackendIsTemp(rnode))
				register_forget_request(rnode, forkNum, segno);

			sprintf(segpath, "%s.%u", path, segno);
			if (unlink(segpath) < 0)
			{
				/* ENOENT is expected after the last segment... */
				if (errno != ENOENT)
					ereport(WARNING,
							(errcode_for_file_access(),
							 errmsg("could not remove file \"%s\": %m", segpath)));
				break;
			}

			if((rnode.node.compress_algorithm != COMPRESS_TYPE_NONE &&
				forkNum == MAIN_FORKNUM))
			{
				char	*pcfile_segpath;

				pcfile_segpath = psprintf("%s_pca", segpath);
				if (unlink(pcfile_segpath) < 0)
				{
					/* ENOENT is expected after the last segment... */
					if (errno != ENOENT)
						ereport(WARNING,
								(errcode_for_file_access(),
								errmsg("could not remove file \"%s\": %m", pcfile_segpath)));
					break;
				}
				pfree(pcfile_segpath);

				pcfile_segpath = psprintf("%s_pcd", segpath);
				if (unlink(pcfile_segpath) < 0)
				{
					/* ENOENT is expected after the last segment... */
					if (errno != ENOENT)
						ereport(WARNING,
								(errcode_for_file_access(),
								errmsg("could not remove file \"%s\": %m", pcfile_segpath)));
					break;
				}
				pfree(pcfile_segpath);
			}
		}
		pfree(segpath);
	}

	pfree(path);
}

/*
 *	mdextend_pc() -- Add a block to the specified page compressed relation.
 *
 */
static void
mdextend_pc(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		 char *buffer, bool skipFsync)
{
	off_t		seekpos;
	int			nbytes;
	MdfdVec    *v;
	char		*work_buffer,*buffer_pos;
	int			i;
	int			prealloc_chunks,need_chunks,chunk_size,nchunks,range,write_amount;
	pc_chunk_number_t chunkno;
	PageCompressHeader	*pcMap;
	PageCompressAddr 	*pcAddr;
	uint8 				algorithm;
	int8 				level;

	/* This assert is too expensive to have on normally ... */
#ifdef CHECK_WRITE_VS_EXTEND
	Assert(blocknum >= mdnblocks(reln, forknum));
#endif

	Assert(IS_COMPRESSED_MAINFORK(reln,forknum));

	/*
	 * If a relation manages to grow to 2^32-1 blocks, refuse to extend it any
	 * more --- we mustn't create a block whose number actually is
	 * InvalidBlockNumber.
	 */
	if (blocknum == InvalidBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("cannot extend file \"%s\" beyond %u blocks",
						relpath(reln->smgr_rnode, forknum),
						InvalidBlockNumber)));

	v = _mdfd_getseg(reln, MAIN_FORKNUM, blocknum, skipFsync, EXTENSION_CREATE);

	chunk_size = PAGE_COMPRESS_CHUNK_SIZE(reln);
	algorithm = PAGE_COMPRESS_ALGORITHM(reln);
	level = PAGE_COMPRESS_LEVEL(reln);
	prealloc_chunks = PAGE_COMPRESS_PREALLOC_CHUNKS(reln);
	if(prealloc_chunks > BLCKSZ / chunk_size -1)
		prealloc_chunks = BLCKSZ / chunk_size -1;

	pcMap = (PageCompressHeader *)GetPageCompressMemoryMap(v->mdfd_vfd_pca, chunk_size);
	pcAddr = GetPageCompressAddr(pcMap, chunk_size, blocknum);

	Assert(blocknum % RELSEG_SIZE >= pg_atomic_read_u32(&pcMap->nblocks));

	/* check allocated chunk number */
	if(pcAddr->allocated_chunks > BLCKSZ / chunk_size)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				errmsg("invalid chunks %u of block %u in file \"%s\"",
						pcAddr->allocated_chunks, blocknum, FilePathName(v->mdfd_vfd_pca))));

	for(i=0; i< pcAddr->allocated_chunks; i++)
	{
		if(pcAddr->chunknos[i] <= 0 || pcAddr->chunknos[i] > (BLCKSZ / chunk_size) * RELSEG_SIZE)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					errmsg("invalid chunk number %u of block %u in file \"%s\"",
							pcAddr->chunknos[i], blocknum, FilePathName(v->mdfd_vfd_pca))));
	}

	nchunks = 0;
	work_buffer = NULL;

	/* compress page only for initialized page */
	if(!PageIsNew(buffer))
	{
		int work_buffer_size, compressed_page_size;

		work_buffer_size = compress_page_buffer_bound(algorithm);
		if(work_buffer_size < 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("unrecognized compression algorithm %d",
							algorithm)));
		work_buffer = palloc(work_buffer_size);

		compressed_page_size = compress_page(buffer, work_buffer, work_buffer_size, algorithm, level);

		if(compressed_page_size < 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("unrecognized compression algorithm %d",
							algorithm)));

		nchunks = (compressed_page_size + chunk_size - 1) / chunk_size;

		if(chunk_size * nchunks >= BLCKSZ)
		{
			/* store original page if can not save space ?TODO? */
			pfree(work_buffer);
			work_buffer = buffer;
			nchunks = BLCKSZ / chunk_size;
		}
		else
		{
			/* fill zero in the last chunk */
			if(compressed_page_size < chunk_size * nchunks)
				memset(work_buffer + compressed_page_size, 0x00, chunk_size * nchunks - compressed_page_size);
		}
	}

	need_chunks = prealloc_chunks > nchunks ? prealloc_chunks : nchunks;
	if(pcAddr->allocated_chunks < need_chunks)
	{
		chunkno = (pc_chunk_number_t)pg_atomic_fetch_add_u32(&pcMap->allocated_chunks, need_chunks - pcAddr->allocated_chunks) + 1;
		for(i = pcAddr->allocated_chunks ;i<need_chunks ;i++,chunkno++)
		{
			pcAddr->chunknos[i] = chunkno;
		}
		pcAddr->allocated_chunks = need_chunks;

		if(compress_address_flush_chunks > 0 &&
		   pg_atomic_read_u32(&pcMap->allocated_chunks) - pg_atomic_read_u32(&pcMap->last_synced_allocated_chunks) > compress_address_flush_chunks)
		{
			if(sync_pcmap(pcMap, WAIT_EVENT_COMPRESS_ADDRESS_FILE_FLUSH) != 0)
				ereport(data_sync_elevel(ERROR),
						(errcode_for_file_access(),
							errmsg("could not msync file \"%s\": %m",
								FilePathName(v->mdfd_vfd_pca))));
		}
	}

	/* write chunks of compressed page */
	for(i=0; i < nchunks;i++)
	{
		buffer_pos = work_buffer + chunk_size * i;
		seekpos = (off_t) OffsetOfPageCompressChunk(chunk_size, pcAddr->chunknos[i]);
		range = 1;
		while(i < nchunks -1 && pcAddr->chunknos[i+1] == pcAddr->chunknos[i] + 1)
		{
			range++;
			i++;
		}
		write_amount = chunk_size * range;

		if ((nbytes = FileWrite(v->mdfd_vfd_pcd, buffer_pos, write_amount, seekpos, WAIT_EVENT_DATA_FILE_EXTEND)) != write_amount)
		{
			if (nbytes < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						errmsg("could not extend file \"%s\": %m",
								FilePathName(v->mdfd_vfd_pcd)),
						errhint("Check free disk space.")));
			/* short write: complain appropriately */
			ereport(ERROR,
					(errcode(ERRCODE_DISK_FULL),
					errmsg("could not extend file \"%s\": wrote only %d of %d bytes at block %u",
							FilePathName(v->mdfd_vfd_pcd),
							nbytes, write_amount, blocknum),
					errhint("Check free disk space.")));
		}
	}

	/* write preallocated chunks */
	if(need_chunks > nchunks)
	{
		char *zero_buffer = palloc0(chunk_size * (need_chunks - nchunks));

		for(i=nchunks; i < need_chunks;i++)
		{
			seekpos = (off_t) OffsetOfPageCompressChunk(chunk_size, pcAddr->chunknos[i]);
			range = 1;
			while(i < nchunks -1 && pcAddr->chunknos[i+1] == pcAddr->chunknos[i] + 1)
			{
				range++;
				i++;
			}
			write_amount = chunk_size * range;

			if ((nbytes = FileWrite(v->mdfd_vfd_pcd, zero_buffer, write_amount, seekpos, WAIT_EVENT_DATA_FILE_EXTEND)) != write_amount)
			{
				if (nbytes < 0)
					ereport(ERROR,
							(errcode_for_file_access(),
							errmsg("could not extend file \"%s\": %m",
									FilePathName(v->mdfd_vfd_pcd)),
							errhint("Check free disk space.")));
				/* short write: complain appropriately */
				ereport(ERROR,
						(errcode(ERRCODE_DISK_FULL),
						errmsg("could not extend file \"%s\": wrote only %d of %d bytes at block %u",
								FilePathName(v->mdfd_vfd_pcd),
								nbytes, write_amount, blocknum),
						errhint("Check free disk space.")));
			}
		}
		pfree(zero_buffer);
	}

	/* finally update size of this page and global nblocks */
	if(pcAddr->nchunks != nchunks)
		pcAddr->nchunks = nchunks;

	if(pg_atomic_read_u32(&pcMap->nblocks) < blocknum % RELSEG_SIZE + 1)
		pg_atomic_write_u32(&pcMap->nblocks, blocknum % RELSEG_SIZE + 1);

	if(work_buffer != NULL && work_buffer != buffer)
		pfree(work_buffer);

	if (!skipFsync && !SmgrIsTemp(reln))
		register_dirty_segment(reln, forknum, v);

	Assert(_mdnblocks(reln, forknum, v) <= ((BlockNumber) RELSEG_SIZE));
}

/*
 *	mdextend() -- Add a block to the specified relation.
 *
 *		The semantics are nearly the same as mdwrite(): write at the
 *		specified position.  However, this is to be used for the case of
 *		extending a relation (i.e., blocknum is at or beyond the current
 *		EOF).  Note that we assume writing a block beyond current EOF
 *		causes intervening file space to become filled with zeroes.
 */
void
mdextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		 char *buffer, bool skipFsync)
{
	off_t		seekpos;
	int			nbytes;
	MdfdVec    *v;

	/* This assert is too expensive to have on normally ... */
#ifdef CHECK_WRITE_VS_EXTEND
	Assert(blocknum >= mdnblocks(reln, forknum));
#endif

	if(IS_COMPRESSED_MAINFORK(reln,forknum))
		return mdextend_pc(reln, forknum, blocknum, buffer, skipFsync);

	/*
	 * If a relation manages to grow to 2^32-1 blocks, refuse to extend it any
	 * more --- we mustn't create a block whose number actually is
	 * InvalidBlockNumber.
	 */
	if (blocknum == InvalidBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("cannot extend file \"%s\" beyond %u blocks",
						relpath(reln->smgr_rnode, forknum),
						InvalidBlockNumber)));

	v = _mdfd_getseg(reln, forknum, blocknum, skipFsync, EXTENSION_CREATE);

	seekpos = (off_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));

	Assert(seekpos < (off_t) BLCKSZ * RELSEG_SIZE);

	if ((nbytes = FileWrite(v->mdfd_vfd, buffer, BLCKSZ, seekpos, WAIT_EVENT_DATA_FILE_EXTEND)) != BLCKSZ)
	{
		if (nbytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					errmsg("could not extend file \"%s\": %m",
							FilePathName(v->mdfd_vfd)),
					errhint("Check free disk space.")));
		/* short write: complain appropriately */
		ereport(ERROR,
				(errcode(ERRCODE_DISK_FULL),
				errmsg("could not extend file \"%s\": wrote only %d of %d bytes at block %u",
						FilePathName(v->mdfd_vfd),
						nbytes, BLCKSZ, blocknum),
				errhint("Check free disk space.")));
	}

	if (!skipFsync && !SmgrIsTemp(reln))
		register_dirty_segment(reln, forknum, v);

	Assert(_mdnblocks(reln, forknum, v) <= ((BlockNumber) RELSEG_SIZE));
}

/*
 *	mdopenfork() -- Open one fork of the specified relation.
 *
 * Note we only open the first segment, when there are multiple segments.
 *
 * If first segment is not present, either ereport or return NULL according
 * to "behavior".  We treat EXTENSION_CREATE the same as EXTENSION_FAIL;
 * EXTENSION_CREATE means it's OK to extend an existing relation, not to
 * invent one out of whole cloth.
 */
static MdfdVec *
mdopenfork(SMgrRelation reln, ForkNumber forknum, int behavior)
{
	MdfdVec    *mdfd;
	char	   *path;
	File		fd,fd_pca,fd_pcd;

	/* No work if already open */
	if (reln->md_num_open_segs[forknum] > 0)
		return &reln->md_seg_fds[forknum][0];

	fd = -1;
	fd_pca = -1;
	fd_pcd = -1;
	if(IS_COMPRESSED_MAINFORK(reln,forknum))
	{
		char	   *pcfile_path;

		path = relpath(reln->smgr_rnode, forknum);

		/* open page compression address file */
		pcfile_path = psprintf("%s_pca", path);
		fd_pca = PathNameOpenFile(pcfile_path, O_RDWR | PG_BINARY);

		if (fd_pca < 0)
		{
			if ((behavior & EXTENSION_RETURN_NULL) &&
				FILE_POSSIBLY_DELETED(errno))
			{
				pfree(path);
				pfree(pcfile_path);
				return NULL;
			}
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\": %m", pcfile_path)));
		}
		pfree(pcfile_path);

		/* open page compression data file */
		pcfile_path = psprintf("%s_pcd", path);
		fd_pcd = PathNameOpenFile(pcfile_path, O_RDWR | PG_BINARY);

		if (fd_pcd < 0)
		{
			if ((behavior & EXTENSION_RETURN_NULL) &&
				FILE_POSSIBLY_DELETED(errno))
			{
				pfree(path);
				pfree(pcfile_path);
				return NULL;
			}
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\": %m", pcfile_path)));
		}
		pfree(pcfile_path);

		SetupPageCompressMemoryMap(fd_pca, PAGE_COMPRESS_CHUNK_SIZE(reln), PAGE_COMPRESS_ALGORITHM(reln));
	}
	else
	{
		path = relpath(reln->smgr_rnode, forknum);

		fd = PathNameOpenFile(path, O_RDWR | PG_BINARY);

		if (fd < 0)
		{
			if ((behavior & EXTENSION_RETURN_NULL) &&
				FILE_POSSIBLY_DELETED(errno))
			{
				pfree(path);
				return NULL;
			}
			ereport(ERROR,
					(errcode_for_file_access(),
					errmsg("could not open file \"%s\": %m", path)));
		}
	}

	pfree(path);

	_fdvec_resize(reln, forknum, 1);
	mdfd = &reln->md_seg_fds[forknum][0];
	mdfd->mdfd_vfd = fd;
	mdfd->mdfd_vfd_pca = fd_pca;
	mdfd->mdfd_vfd_pcd = fd_pcd;
	mdfd->mdfd_segno = 0;

	Assert(_mdnblocks(reln, forknum, mdfd) <= ((BlockNumber) RELSEG_SIZE));

	return mdfd;
}

/*
 *  mdopen() -- Initialize newly-opened relation.
 */
void
mdopen(SMgrRelation reln)
{
	/* mark it not open */
	for (int forknum = 0; forknum <= MAX_FORKNUM; forknum++)
		reln->md_num_open_segs[forknum] = 0;
}

/*
 *	mdclose() -- Close the specified relation, if it isn't closed already.
 */
void
mdclose(SMgrRelation reln, ForkNumber forknum)
{
	int			nopensegs = reln->md_num_open_segs[forknum];

	/* No work if already closed */
	if (nopensegs == 0)
		return;

	/* close segments starting from the end */
	while (nopensegs > 0)
	{
		MdfdVec    *v = &reln->md_seg_fds[forknum][nopensegs - 1];

		if(IS_COMPRESSED_MAINFORK(reln,forknum))
		{
			FileClose(v->mdfd_vfd_pca);
			FileClose(v->mdfd_vfd_pcd);
		}
		else
			FileClose(v->mdfd_vfd);
		_fdvec_resize(reln, forknum, nopensegs - 1);
		nopensegs--;
	}
}

/*
 *	mdprefetch() -- Initiate asynchronous read of the specified block of a relation
 */
bool
mdprefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
#ifdef USE_PREFETCH
	off_t		seekpos;
	MdfdVec    *v;

	v = _mdfd_getseg(reln, forknum, blocknum, false,
					 InRecovery ? EXTENSION_RETURN_NULL : EXTENSION_FAIL);
	if (v == NULL)
		return false;

	if(IS_COMPRESSED_MAINFORK(reln,forknum))
	{
		int					chunk_size,i,range;
		PageCompressHeader	*pcMap;
		PageCompressAddr	*pcAddr;
		
		chunk_size = PAGE_COMPRESS_CHUNK_SIZE(reln);
		pcMap = (PageCompressHeader *)GetPageCompressMemoryMap(v->mdfd_vfd_pca, chunk_size);
		pcAddr = GetPageCompressAddr(pcMap, chunk_size, blocknum);

		/* check chunk number */
		if(pcAddr->nchunks < 0 || pcAddr->nchunks > BLCKSZ / chunk_size)
		{
			if (zero_damaged_pages || InRecovery)
				return true;
			else
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("invalid chunks %u of block %u in file \"%s\"",
								pcAddr->nchunks, blocknum, FilePathName(v->mdfd_vfd_pca))));
		}

		for(i=0; i< pcAddr->nchunks; i++)
		{
			if(pcAddr->chunknos[i] <= 0 || pcAddr->chunknos[i] > (BLCKSZ / chunk_size) * RELSEG_SIZE)
			{
				if (zero_damaged_pages || InRecovery)
					return true;
				else
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("invalid chunk number %u of block %u in file \"%s\"",
									pcAddr->chunknos[i], blocknum, FilePathName(v->mdfd_vfd_pca))));
			}

			seekpos = (off_t) OffsetOfPageCompressChunk(chunk_size, pcAddr->chunknos[i]);
			range = 1;
			while(i < pcAddr->nchunks - 1 && 
					pcAddr->chunknos[i + 1] == pcAddr->chunknos[i] + 1)
			{
				range++;
				i++;
			}

			(void) FilePrefetch(v->mdfd_vfd_pcd, seekpos, chunk_size * range, WAIT_EVENT_DATA_FILE_PREFETCH);
		}
	}
	else
	{
		seekpos = (off_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));

		Assert(seekpos < (off_t) BLCKSZ * RELSEG_SIZE);

		(void) FilePrefetch(v->mdfd_vfd, seekpos, BLCKSZ, WAIT_EVENT_DATA_FILE_PREFETCH);
	}
#endif							/* USE_PREFETCH */

	return true;
}

/*
 * mdwriteback() -- Tell the kernel to write pages back to storage.
 *
 * This accepts a range of blocks because flushing several pages at once is
 * considerably more efficient than doing so individually.
 */
void
mdwriteback(SMgrRelation reln, ForkNumber forknum,
			BlockNumber blocknum, BlockNumber nblocks)
{
	/*
	 * Issue flush requests in as few requests as possible; have to split at
	 * segment boundaries though, since those are actually separate files.
	 */
	while (nblocks > 0)
	{
		BlockNumber nflush = nblocks;
		off_t		seekpos;
		MdfdVec    *v;
		int			segnum_start,
					segnum_end;

		v = _mdfd_getseg(reln, forknum, blocknum, true /* not used */ ,
						 EXTENSION_RETURN_NULL);

		/*
		 * We might be flushing buffers of already removed relations, that's
		 * ok, just ignore that case.
		 */
		if (!v)
			return;

		/* compute offset inside the current segment */
		segnum_start = blocknum / RELSEG_SIZE;

		/* compute number of desired writes within the current segment */
		segnum_end = (blocknum + nblocks - 1) / RELSEG_SIZE;
		if (segnum_start != segnum_end)
			nflush = RELSEG_SIZE - (blocknum % ((BlockNumber) RELSEG_SIZE));

		Assert(nflush >= 1);
		Assert(nflush <= nblocks);

		if(IS_COMPRESSED_MAINFORK(reln,forknum))
		{
			int					i,chunk_size;
			PageCompressHeader	*pcMap;
			PageCompressAddr	*pcAddr;
			BlockNumber			iblock;
			pc_chunk_number_t	seekpos_chunk,last_chunk,nchunks;
			
			chunk_size = PAGE_COMPRESS_CHUNK_SIZE(reln);
			pcMap = (PageCompressHeader *)GetPageCompressMemoryMap(v->mdfd_vfd_pca, chunk_size);

			seekpos_chunk = -1;
			last_chunk = -1;
			for(iblock = 0; iblock < nflush; iblock++)
			{
				/* flush one block */
				pcAddr = GetPageCompressAddr(pcMap, chunk_size, blocknum + iblock);

				for(i=0; i < pcAddr->nchunks; i++)
				{
					if(seekpos_chunk == -1)
					{
						seekpos_chunk = pcAddr->chunknos[i];
						last_chunk = seekpos_chunk;
					}
					else if(pcAddr->chunknos[i] == last_chunk + 1)
					{
						last_chunk++;
					}
					else
					{
						/* from here the chunks is discontinuous, flush previous chuncks range */
						seekpos = (off_t) OffsetOfPageCompressChunk(chunk_size, seekpos_chunk);
						nchunks = 1 + last_chunk - seekpos_chunk;

						FileWriteback(v->mdfd_vfd_pcd, seekpos, (off_t) chunk_size * nchunks, WAIT_EVENT_DATA_FILE_FLUSH);

						seekpos_chunk = pcAddr->chunknos[i];
						last_chunk = seekpos_chunk;
					}
				}
			}

			/* flush the rest chuncks */
			if(seekpos_chunk != -1)
			{
				seekpos = (off_t) OffsetOfPageCompressChunk(chunk_size, seekpos_chunk);
				nchunks = 1 + last_chunk - seekpos_chunk;

				FileWriteback(v->mdfd_vfd_pcd, seekpos, (off_t) chunk_size * nchunks, WAIT_EVENT_DATA_FILE_FLUSH);
			}
		}
		else
		{
			seekpos = (off_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));

			FileWriteback(v->mdfd_vfd, seekpos, (off_t) BLCKSZ * nflush, WAIT_EVENT_DATA_FILE_FLUSH);
		}

		nblocks -= nflush;
		blocknum += nflush;
	}
}

/*
 *	mdread_pc() -- Read the specified block from a page compressed relation.
 */
static void
mdread_pc(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
	   char *buffer)
{
	off_t		seekpos;
	int			nbytes,chunk_size,i,read_amount,range,nchunks;
	MdfdVec    *v;
	PageCompressHeader	*pcMap;
	PageCompressAddr	*pcAddr;
	char		*compress_buffer,*buffer_pos;

	Assert(IS_COMPRESSED_MAINFORK(reln,forkNum));

	v = _mdfd_getseg(reln, forknum, blocknum, false,
					EXTENSION_FAIL | EXTENSION_CREATE_RECOVERY);

	chunk_size = PAGE_COMPRESS_CHUNK_SIZE(reln);
	pcMap = (PageCompressHeader *)GetPageCompressMemoryMap(v->mdfd_vfd_pca, chunk_size);
	pcAddr = GetPageCompressAddr(pcMap, chunk_size, blocknum);

	nchunks = pcAddr->nchunks;
	if(nchunks == 0)
	{
		MemSet(buffer, 0, BLCKSZ);
		return;
	}

	/* check chunk number */
	if(nchunks > BLCKSZ / chunk_size)
	{
		if (zero_damaged_pages || InRecovery)
		{
			MemSet(buffer, 0, BLCKSZ);
			return;
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					errmsg("invalid chunks %u of block %u in file \"%s\"",
							nchunks, blocknum, FilePathName(v->mdfd_vfd_pca))));
	}

	for(i=0; i< nchunks; i++)
	{
		if(pcAddr->chunknos[i] <= 0 || pcAddr->chunknos[i] > MAX_CHUNK_NUMBER(chunk_size))
		{
			if (zero_damaged_pages || InRecovery)
			{
				MemSet(buffer, 0, BLCKSZ);
				return;
			}
			else
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("invalid chunk number %u of block %u in file \"%s\"",
								pcAddr->chunknos[i], blocknum, FilePathName(v->mdfd_vfd_pca))));
		}
	}

	/* read chunk data */
	compress_buffer = palloc(chunk_size * nchunks);
	for(i=0; i< nchunks; i++)
	{
		buffer_pos = compress_buffer + chunk_size * i;
		seekpos = (off_t) OffsetOfPageCompressChunk(chunk_size, pcAddr->chunknos[i]);
		range = 1;
		while(i<nchunks-1 && pcAddr->chunknos[i+1] == pcAddr->chunknos[i]+1)
		{
			range++;
			i++;
		}
		read_amount = chunk_size * range;

		TRACE_POSTGRESQL_SMGR_MD_READ_START(forknum, blocknum,
											reln->smgr_rnode.node.spcNode,
											reln->smgr_rnode.node.dbNode,
											reln->smgr_rnode.node.relNode,
											reln->smgr_rnode.backend);

		nbytes = FileRead(v->mdfd_vfd_pcd, buffer_pos, read_amount, seekpos, WAIT_EVENT_DATA_FILE_READ);

		TRACE_POSTGRESQL_SMGR_MD_READ_DONE(forknum, blocknum,
										reln->smgr_rnode.node.spcNode,
										reln->smgr_rnode.node.dbNode,
										reln->smgr_rnode.node.relNode,
										reln->smgr_rnode.backend,
										nbytes,
										read_amount);

		if (nbytes != read_amount)
		{
			if (nbytes < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						errmsg("could not read block %u in file \"%s\": %m",
								blocknum, FilePathName(v->mdfd_vfd_pcd))));

			/*
			* Short read: we are at or past EOF, or we read a partial block at
			* EOF.  Normally this is an error; upper levels should never try to
			* read a nonexistent block.  However, if zero_damaged_pages is ON or
			* we are InRecovery, we should instead return zeroes without
			* complaining.  This allows, for example, the case of trying to
			* update a block that was later truncated away.
			*/
			if (zero_damaged_pages || InRecovery)
			{
				pfree(compress_buffer);
				MemSet(buffer, 0, BLCKSZ);
				return;
			}
			else
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("could not read block %u in file \"%s\": read only %d of %d bytes",
								blocknum, FilePathName(v->mdfd_vfd_pcd),
								nbytes, read_amount)));
		}
	}

	/* decompress chunk data */
	if(pcAddr->nchunks == BLCKSZ / chunk_size)
	{
		memcpy(buffer, compress_buffer, BLCKSZ);
	}
	else
	{
		nbytes = decompress_page(compress_buffer, buffer, PAGE_COMPRESS_ALGORITHM(reln) );
		if (nbytes != BLCKSZ)
		{
			if(nbytes == -2)
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("could not recognized compression algorithm %d for file \"%s\"",
								PAGE_COMPRESS_ALGORITHM(reln),
								FilePathName(v->mdfd_vfd_pcd))));

			/*
			* Short read: we are at or past EOF, or we read a partial block at
			* EOF.  Normally this is an error; upper levels should never try to
			* read a nonexistent block.  However, if zero_damaged_pages is ON or
			* we are InRecovery, we should instead return zeroes without
			* complaining.  This allows, for example, the case of trying to
			* update a block that was later truncated away.
			*/
			if (zero_damaged_pages || InRecovery)
			{
				pfree(compress_buffer);
				MemSet(buffer, 0, BLCKSZ);
				return;
			}
			else
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("could not decompress block %u in file \"%s\": decompress %d of %d bytes",
								blocknum, FilePathName(v->mdfd_vfd_pcd),
								nbytes, BLCKSZ)));
		}
	}

	pfree(compress_buffer);
}

/*
 *	mdread() -- Read the specified block from a relation.
 */
void
mdread(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
	   char *buffer)
{
	off_t		seekpos;
	int			nbytes;
	MdfdVec    *v;

	if(IS_COMPRESSED_MAINFORK(reln,forknum))
		return mdread_pc(reln, forknum, blocknum, buffer);

	TRACE_POSTGRESQL_SMGR_MD_READ_START(forknum, blocknum,
										reln->smgr_rnode.node.spcNode,
										reln->smgr_rnode.node.dbNode,
										reln->smgr_rnode.node.relNode,
										reln->smgr_rnode.backend);

	v = _mdfd_getseg(reln, forknum, blocknum, false,
					EXTENSION_FAIL | EXTENSION_CREATE_RECOVERY);

	seekpos = (off_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));

	Assert(seekpos < (off_t) BLCKSZ * RELSEG_SIZE);

	nbytes = FileRead(v->mdfd_vfd, buffer, BLCKSZ, seekpos, WAIT_EVENT_DATA_FILE_READ);

	TRACE_POSTGRESQL_SMGR_MD_READ_DONE(forknum, blocknum,
									reln->smgr_rnode.node.spcNode,
									reln->smgr_rnode.node.dbNode,
									reln->smgr_rnode.node.relNode,
									reln->smgr_rnode.backend,
									nbytes,
									BLCKSZ);

	if (nbytes != BLCKSZ)
	{
		if (nbytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					errmsg("could not read block %u in file \"%s\": %m",
							blocknum, FilePathName(v->mdfd_vfd))));

		/*
		* Short read: we are at or past EOF, or we read a partial block at
		* EOF.  Normally this is an error; upper levels should never try to
		* read a nonexistent block.  However, if zero_damaged_pages is ON or
		* we are InRecovery, we should instead return zeroes without
		* complaining.  This allows, for example, the case of trying to
		* update a block that was later truncated away.
		*/
		if (zero_damaged_pages || InRecovery)
			MemSet(buffer, 0, BLCKSZ);
		else
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					errmsg("could not read block %u in file \"%s\": read only %d of %d bytes",
							blocknum, FilePathName(v->mdfd_vfd),
							nbytes, BLCKSZ)));
	}
}

/*
 *	mdwrite_pc() -- Write the supplied block at the appropriate location for page compressed relation.
 *
 *		This is to be used only for updating already-existing blocks of a
 *		relation (ie, those before the current EOF).  To extend a relation,
 *		use mdextend().
 */
static void
mdwrite_pc(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		 char *buffer, bool skipFsync)
{
	off_t		seekpos;
	int			nbytes;
	MdfdVec    *v;
	char		*work_buffer,*buffer_pos;
	int			i,work_buffer_size, compressed_page_size;
	int			prealloc_chunks,chunk_size,nchunks,need_chunks,range,write_amount;
	pc_chunk_number_t chunkno;
	PageCompressHeader	*pcMap;
	PageCompressAddr 	*pcAddr;
	uint8 				algorithm;
	int8				level;

	/* This assert is too expensive to have on normally ... */
#ifdef CHECK_WRITE_VS_EXTEND
	Assert(blocknum < mdnblocks(reln, forknum));
#endif

	Assert(IS_COMPRESSED_MAINFORK(reln,forkNum));

	v = _mdfd_getseg(reln, forknum, blocknum, skipFsync,
					 EXTENSION_FAIL | EXTENSION_CREATE_RECOVERY);

	chunk_size = PAGE_COMPRESS_CHUNK_SIZE(reln);
	algorithm = PAGE_COMPRESS_ALGORITHM(reln);
	level = PAGE_COMPRESS_LEVEL(reln);
	prealloc_chunks = PAGE_COMPRESS_PREALLOC_CHUNKS(reln);
	if(prealloc_chunks > BLCKSZ / chunk_size -1)
		prealloc_chunks = BLCKSZ / chunk_size -1;

	pcMap = (PageCompressHeader *)GetPageCompressMemoryMap(v->mdfd_vfd_pca, chunk_size);
	pcAddr = GetPageCompressAddr(pcMap, chunk_size, blocknum);

	Assert(blocknum % RELSEG_SIZE < pg_atomic_read_u32(&pcMap->nblocks));

	/* check allocated chunk number */
	if(pcAddr->allocated_chunks > BLCKSZ / chunk_size)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				errmsg("invalid chunks %u of block %u in file \"%s\"",
						pcAddr->allocated_chunks, blocknum, FilePathName(v->mdfd_vfd_pca))));

	for(i=0; i< pcAddr->allocated_chunks; i++)
	{
		if(pcAddr->chunknos[i] <= 0 || pcAddr->chunknos[i] > MAX_CHUNK_NUMBER(chunk_size))
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					errmsg("invalid chunk number %u of block %u in file \"%s\"",
							pcAddr->chunknos[i], blocknum, FilePathName(v->mdfd_vfd_pca))));
	}

	/* compress page */
	work_buffer_size = compress_page_buffer_bound(algorithm);
	if(work_buffer_size < 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("unrecognized compression algorithm %d",
						algorithm)));
	work_buffer = palloc(work_buffer_size);

	compressed_page_size = compress_page(buffer, work_buffer, work_buffer_size, algorithm, level);

	if(compressed_page_size < 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("unrecognized compression algorithm %d",
						algorithm)));

	nchunks = (compressed_page_size + chunk_size - 1) / chunk_size;

	if(chunk_size * nchunks >= BLCKSZ)
	{
		/* store original page if can not save space ?TODO? */
		pfree(work_buffer);
		work_buffer = buffer;
		nchunks = BLCKSZ / chunk_size;
	}
	else
	{
		/* fill zero in the last chunk */
		if(compressed_page_size < chunk_size * nchunks)
			memset(work_buffer + compressed_page_size, 0x00, chunk_size * nchunks - compressed_page_size);
	}

	need_chunks = prealloc_chunks > nchunks ? prealloc_chunks : nchunks;

	/* allocate chunks needed */
	if(pcAddr->allocated_chunks < need_chunks)
	{
		chunkno = (pc_chunk_number_t)pg_atomic_fetch_add_u32(&pcMap->allocated_chunks, need_chunks - pcAddr->allocated_chunks) + 1;
		for(i = pcAddr->allocated_chunks ;i<need_chunks ;i++,chunkno++)
		{
			pcAddr->chunknos[i] = chunkno;
		}
		pcAddr->allocated_chunks = need_chunks;

		if(compress_address_flush_chunks > 0 &&
		   pg_atomic_read_u32(&pcMap->allocated_chunks) - pg_atomic_read_u32(&pcMap->last_synced_allocated_chunks) > compress_address_flush_chunks)
		{
			if(sync_pcmap(pcMap, WAIT_EVENT_COMPRESS_ADDRESS_FILE_FLUSH) != 0)
				ereport(data_sync_elevel(ERROR),
						(errcode_for_file_access(),
							errmsg("could not msync file \"%s\": %m",
								FilePathName(v->mdfd_vfd_pca))));
		}
	}

	/* write chunks of compressed page */
	for(i=0; i < nchunks;i++)
	{
		buffer_pos = work_buffer + chunk_size * i;
		seekpos = (off_t) OffsetOfPageCompressChunk(chunk_size, pcAddr->chunknos[i]);
		range = 1;
		while(i < nchunks -1 && pcAddr->chunknos[i+1] == pcAddr->chunknos[i] + 1)
		{
			range++;
			i++;
		}
		write_amount = chunk_size * range;

		TRACE_POSTGRESQL_SMGR_MD_WRITE_START(forknum, blocknum,
											reln->smgr_rnode.node.spcNode,
											reln->smgr_rnode.node.dbNode,
											reln->smgr_rnode.node.relNode,
											reln->smgr_rnode.backend);

		nbytes = FileWrite(v->mdfd_vfd_pcd, buffer_pos, write_amount, seekpos, WAIT_EVENT_DATA_FILE_EXTEND);

		TRACE_POSTGRESQL_SMGR_MD_WRITE_DONE(forknum, blocknum,
											reln->smgr_rnode.node.spcNode,
											reln->smgr_rnode.node.dbNode,
											reln->smgr_rnode.node.relNode,
											reln->smgr_rnode.backend,
											nbytes,
											write_amount);

		if (nbytes != write_amount)
		{
			if (nbytes < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						errmsg("could not write block %u in file \"%s\": %m",
								blocknum, FilePathName(v->mdfd_vfd_pcd))));
			/* short write: complain appropriately */
			ereport(ERROR,
					(errcode(ERRCODE_DISK_FULL),
					errmsg("could not write block %u in file \"%s\": wrote only %d of %d bytes",
							blocknum,
							FilePathName(v->mdfd_vfd_pcd),
							nbytes, write_amount),
					errhint("Check free disk space.")));
		}
	}

	/* write preallocated chunks */
	if(need_chunks > nchunks)
	{
		char *zero_buffer = palloc0(chunk_size * (need_chunks - nchunks));

		for(i=nchunks; i < need_chunks;i++)
		{
			seekpos = (off_t) OffsetOfPageCompressChunk(chunk_size, pcAddr->chunknos[i]);
			range = 1;
			while(i < nchunks -1 && pcAddr->chunknos[i+1] == pcAddr->chunknos[i] + 1)
			{
				range++;
				i++;
			}
			write_amount = chunk_size * range;

			TRACE_POSTGRESQL_SMGR_MD_WRITE_START(forknum, blocknum,
												reln->smgr_rnode.node.spcNode,
												reln->smgr_rnode.node.dbNode,
												reln->smgr_rnode.node.relNode,
												reln->smgr_rnode.backend);

			nbytes = FileWrite(v->mdfd_vfd_pcd, zero_buffer, write_amount, seekpos, WAIT_EVENT_DATA_FILE_EXTEND);

			TRACE_POSTGRESQL_SMGR_MD_WRITE_DONE(forknum, blocknum,
												reln->smgr_rnode.node.spcNode,
												reln->smgr_rnode.node.dbNode,
												reln->smgr_rnode.node.relNode,
												reln->smgr_rnode.backend,
												nbytes,
												write_amount);

			if (nbytes != write_amount)
			{
				if (nbytes < 0)
					ereport(ERROR,
							(errcode_for_file_access(),
							errmsg("could not extend file \"%s\": %m",
									FilePathName(v->mdfd_vfd_pcd)),
							errhint("Check free disk space.")));
				/* short write: complain appropriately */
				ereport(ERROR,
						(errcode(ERRCODE_DISK_FULL),
						errmsg("could not extend file \"%s\": wrote only %d of %d bytes at block %u",
								FilePathName(v->mdfd_vfd_pcd),
								nbytes, write_amount, blocknum),
						errhint("Check free disk space.")));
			}
		}
		pfree(zero_buffer);
	}

	/* finally update size of this page and global nblocks */
	if(pcAddr->nchunks != nchunks)
		pcAddr->nchunks = nchunks;

	if(work_buffer != buffer)
		pfree(work_buffer);

	if (!skipFsync && !SmgrIsTemp(reln))
		register_dirty_segment(reln, forknum, v);
}

/*
 *	mdwrite() -- Write the supplied block at the appropriate location.
 *
 *		This is to be used only for updating already-existing blocks of a
 *		relation (ie, those before the current EOF).  To extend a relation,
 *		use mdextend().
 */
void
mdwrite(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		char *buffer, bool skipFsync)
{
	off_t		seekpos;
	int			nbytes;
	MdfdVec    *v;

	/* This assert is too expensive to have on normally ... */
#ifdef CHECK_WRITE_VS_EXTEND
	Assert(blocknum < mdnblocks(reln, forknum));
#endif

	if(IS_COMPRESSED_MAINFORK(reln,forknum))
		return mdwrite_pc(reln, forknum, blocknum, buffer, skipFsync);

	TRACE_POSTGRESQL_SMGR_MD_WRITE_START(forknum, blocknum,
										 reln->smgr_rnode.node.spcNode,
										 reln->smgr_rnode.node.dbNode,
										 reln->smgr_rnode.node.relNode,
										 reln->smgr_rnode.backend);

	v = _mdfd_getseg(reln, forknum, blocknum, skipFsync,
					 EXTENSION_FAIL | EXTENSION_CREATE_RECOVERY);

	seekpos = (off_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));

	Assert(seekpos < (off_t) BLCKSZ * RELSEG_SIZE);

	nbytes = FileWrite(v->mdfd_vfd, buffer, BLCKSZ, seekpos, WAIT_EVENT_DATA_FILE_WRITE);

	TRACE_POSTGRESQL_SMGR_MD_WRITE_DONE(forknum, blocknum,
										reln->smgr_rnode.node.spcNode,
										reln->smgr_rnode.node.dbNode,
										reln->smgr_rnode.node.relNode,
										reln->smgr_rnode.backend,
										nbytes,
										BLCKSZ);

	if (nbytes != BLCKSZ)
	{
		if (nbytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write block %u in file \"%s\": %m",
							blocknum, FilePathName(v->mdfd_vfd))));
		/* short write: complain appropriately */
		ereport(ERROR,
				(errcode(ERRCODE_DISK_FULL),
				 errmsg("could not write block %u in file \"%s\": wrote only %d of %d bytes",
						blocknum,
						FilePathName(v->mdfd_vfd),
						nbytes, BLCKSZ),
				 errhint("Check free disk space.")));
	}

	if (!skipFsync && !SmgrIsTemp(reln))
		register_dirty_segment(reln, forknum, v);
}

/*
 *	mdnblocks() -- Get the number of blocks stored in a relation.
 *
 *		Important side effect: all active segments of the relation are opened
 *		and added to the md_seg_fds array.  If this routine has not been
 *		called, then only segments up to the last one actually touched
 *		are present in the array.
 */
BlockNumber
mdnblocks(SMgrRelation reln, ForkNumber forknum)
{
	MdfdVec    *v;
	BlockNumber nblocks;
	BlockNumber segno;

	mdopenfork(reln, forknum, EXTENSION_FAIL);

	/* mdopen has opened the first segment */
	Assert(reln->md_num_open_segs[forknum] > 0);

	/*
	 * Start from the last open segments, to avoid redundant seeks.  We have
	 * previously verified that these segments are exactly RELSEG_SIZE long,
	 * and it's useless to recheck that each time.
	 *
	 * NOTE: this assumption could only be wrong if another backend has
	 * truncated the relation.  We rely on higher code levels to handle that
	 * scenario by closing and re-opening the md fd, which is handled via
	 * relcache flush.  (Since the checkpointer doesn't participate in
	 * relcache flush, it could have segment entries for inactive segments;
	 * that's OK because the checkpointer never needs to compute relation
	 * size.)
	 */
	segno = reln->md_num_open_segs[forknum] - 1;
	v = &reln->md_seg_fds[forknum][segno];

	for (;;)
	{
		nblocks = _mdnblocks(reln, forknum, v);
		if (nblocks > ((BlockNumber) RELSEG_SIZE))
			elog(FATAL, "segment too big");
		if (nblocks < ((BlockNumber) RELSEG_SIZE))
			return (segno * ((BlockNumber) RELSEG_SIZE)) + nblocks;

		/*
		 * If segment is exactly RELSEG_SIZE, advance to next one.
		 */
		segno++;

		/*
		 * We used to pass O_CREAT here, but that has the disadvantage that it
		 * might create a segment which has vanished through some operating
		 * system misadventure.  In such a case, creating the segment here
		 * undermines _mdfd_getseg's attempts to notice and report an error
		 * upon access to a missing segment.
		 */
		v = _mdfd_openseg(reln, forknum, segno, 0);
		if (v == NULL)
			return segno * ((BlockNumber) RELSEG_SIZE);
	}
}

/*
 *	mdtruncate() -- Truncate relation to specified number of blocks.
 */
void
mdtruncate(SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks)
{
	BlockNumber curnblk;
	BlockNumber priorblocks;
	BlockNumber	blk;
	int			curopensegs, chunk_size, i;
	PageCompressHeader	*pcMap;
	PageCompressAddr	*pcAddr;

	/*
	 * NOTE: mdnblocks makes sure we have opened all active segments, so that
	 * truncation loop will get them all!
	 */
	curnblk = mdnblocks(reln, forknum);
	if (nblocks > curnblk)
	{
		/* Bogus request ... but no complaint if InRecovery */
		if (InRecovery)
			return;
		ereport(ERROR,
				(errmsg("could not truncate file \"%s\" to %u blocks: it's only %u blocks now",
						relpath(reln->smgr_rnode, forknum),
						nblocks, curnblk)));
	}
	if (nblocks == curnblk)
		return;					/* no work */

	/*
	 * Truncate segments, starting at the last one. Starting at the end makes
	 * managing the memory for the fd array easier, should there be errors.
	 */
	curopensegs = reln->md_num_open_segs[forknum];
	while (curopensegs > 0)
	{
		MdfdVec    *v;

		priorblocks = (curopensegs - 1) * RELSEG_SIZE;

		v = &reln->md_seg_fds[forknum][curopensegs - 1];

		if (priorblocks > nblocks)
		{
			/*
			 * This segment is no longer active. We truncate the file, but do
			 * not delete it, for reasons explained in the header comments.
			 */
			if(IS_COMPRESSED_MAINFORK(reln,forknum))
			{
				chunk_size = PAGE_COMPRESS_CHUNK_SIZE(reln);
				pcMap = (PageCompressHeader *)GetPageCompressMemoryMap(v->mdfd_vfd_pca, chunk_size);

				pg_atomic_write_u32(&pcMap->nblocks, 0);
				pg_atomic_write_u32(&pcMap->allocated_chunks, 0);
				memset((char *)pcMap + SizeOfPageCompressHeaderData,
						0x00,
						SizeofPageCompressAddrFile(chunk_size) - SizeOfPageCompressHeaderData);

				if(sync_pcmap(pcMap, WAIT_EVENT_COMPRESS_ADDRESS_FILE_SYNC) != 0)
					ereport(data_sync_elevel(ERROR),
							(errcode_for_file_access(),
								errmsg("could not msync file \"%s\": %m",
									FilePathName(v->mdfd_vfd_pca))));

				if (FileTruncate(v->mdfd_vfd_pcd, 0, WAIT_EVENT_DATA_FILE_TRUNCATE) < 0)
					ereport(ERROR,
							(errcode_for_file_access(),
							errmsg("could not truncate file \"%s\": %m",
									FilePathName(v->mdfd_vfd_pcd))));
			}
			else{
				if (FileTruncate(v->mdfd_vfd, 0, WAIT_EVENT_DATA_FILE_TRUNCATE) < 0)
					ereport(ERROR,
							(errcode_for_file_access(),
							errmsg("could not truncate file \"%s\": %m",
									FilePathName(v->mdfd_vfd))));
			}

			if (!SmgrIsTemp(reln))
				register_dirty_segment(reln, forknum, v);

			/* we never drop the 1st segment */
			Assert(v != &reln->md_seg_fds[forknum][0]);

			if(IS_COMPRESSED_MAINFORK(reln,forknum))
			{
				FileClose(v->mdfd_vfd_pca);
				FileClose(v->mdfd_vfd_pcd);
			}
			else
				FileClose(v->mdfd_vfd);

			_fdvec_resize(reln, forknum, curopensegs - 1);
		}
		else if (priorblocks + ((BlockNumber) RELSEG_SIZE) > nblocks)
		{
			if(IS_COMPRESSED_MAINFORK(reln,forknum))
			{
				pc_chunk_number_t max_used_chunkno = (pc_chunk_number_t) 0;
				BlockNumber lastsegblocks = nblocks - priorblocks;
				uint32 allocated_chunks;

				chunk_size = PAGE_COMPRESS_CHUNK_SIZE(reln);
				pcMap = (PageCompressHeader *)GetPageCompressMemoryMap(v->mdfd_vfd_pca, chunk_size);

				for(blk = lastsegblocks; blk < RELSEG_SIZE; blk++)
				{
					pcAddr = GetPageCompressAddr(pcMap, chunk_size, blk);
					pcAddr->nchunks = 0;
				}

				pg_atomic_write_u32(&pcMap->nblocks, lastsegblocks);

				if(sync_pcmap(pcMap, WAIT_EVENT_COMPRESS_ADDRESS_FILE_SYNC) != 0)
					ereport(data_sync_elevel(ERROR),
							(errcode_for_file_access(),
								errmsg("could not msync file \"%s\": %m",
									FilePathName(v->mdfd_vfd_pca))));

				allocated_chunks = pg_atomic_read_u32(&pcMap->allocated_chunks);

				/* find the max used chunkno */
				for(blk = (BlockNumber)0; blk < (BlockNumber)lastsegblocks; blk++)
				{
					pcAddr = GetPageCompressAddr(pcMap, chunk_size, blk);

					/* check allocated_chunks for one page */
					if(pcAddr->allocated_chunks > BLCKSZ / chunk_size)
					{
						ereport(ERROR,
								(errcode(ERRCODE_DATA_CORRUPTED),
								errmsg("invalid chunks %u of block %u in file \"%s\"",
										pcAddr->allocated_chunks, blk, FilePathName(v->mdfd_vfd_pca))));
					}

					/* check chunknos for one page */
					for(i = 0; i< pcAddr->allocated_chunks; i++)
					{
						if(pcAddr->chunknos[i] == 0 || pcAddr->chunknos[i] > allocated_chunks)
						{
							ereport(ERROR,
									(errcode(ERRCODE_DATA_CORRUPTED),
									errmsg("invalid chunk number %u of block %u in file \"%s\"",
											pcAddr->chunknos[i], blk, FilePathName(v->mdfd_vfd_pca))));
						}

						if(pcAddr->chunknos[i] > max_used_chunkno )
							max_used_chunkno = pcAddr->chunknos[i];
					}
				}

				if (FileTruncate(v->mdfd_vfd_pcd, max_used_chunkno * chunk_size, WAIT_EVENT_DATA_FILE_TRUNCATE) < 0)
					ereport(ERROR,
							(errcode_for_file_access(),
							errmsg("could not truncate file \"%s\": %m",
									FilePathName(v->mdfd_vfd_pcd))));
			}
			else
			{
				/*
				* This is the last segment we want to keep. Truncate the file to
				* the right length. NOTE: if nblocks is exactly a multiple K of
				* RELSEG_SIZE, we will truncate the K+1st segment to 0 length but
				* keep it. This adheres to the invariant given in the header
				* comments.
				*/
				BlockNumber lastsegblocks = nblocks - priorblocks;

				if (FileTruncate(v->mdfd_vfd, (off_t) lastsegblocks * BLCKSZ, WAIT_EVENT_DATA_FILE_TRUNCATE) < 0)
					ereport(ERROR,
							(errcode_for_file_access(),
							errmsg("could not truncate file \"%s\" to %u blocks: %m",
									FilePathName(v->mdfd_vfd),
									nblocks)));
			}
			if (!SmgrIsTemp(reln))
				register_dirty_segment(reln, forknum, v);
		}
		else
		{
			/*
			 * We still need this segment, so nothing to do for this and any
			 * earlier segment.
			 */
			break;
		}
		curopensegs--;
	}
}

/*
 *	mdimmedsync() -- Immediately sync a relation to stable storage.
 *
 * Note that only writes already issued are synced; this routine knows
 * nothing of dirty buffers that may exist inside the buffer manager.  We
 * sync active and inactive segments; smgrDoPendingSyncs() relies on this.
 * Consider a relation skipping WAL.  Suppose a checkpoint syncs blocks of
 * some segment, then mdtruncate() renders that segment inactive.  If we
 * crash before the next checkpoint syncs the newly-inactive segment, that
 * segment may survive recovery, reintroducing unwanted data into the table.
 */
void
mdimmedsync(SMgrRelation reln, ForkNumber forknum)
{
	int			segno;
	int			min_inactive_seg;

	/*
	 * NOTE: mdnblocks makes sure we have opened all active segments, so that
	 * fsync loop will get them all!
	 */
	mdnblocks(reln, forknum);

	min_inactive_seg = segno = reln->md_num_open_segs[forknum];

	/*
	 * Temporarily open inactive segments, then close them after sync.  There
	 * may be some inactive segments left opened after fsync() error, but that
	 * is harmless.  We don't bother to clean them up and take a risk of
	 * further trouble.  The next mdclose() will soon close them.
	 */
	while (_mdfd_openseg(reln, forknum, segno, 0) != NULL)
		segno++;

	while (segno > 0)
	{
		MdfdVec    *v = &reln->md_seg_fds[forknum][segno - 1];

		if(IS_COMPRESSED_MAINFORK(reln,forknum))
		{
			PageCompressHeader *pcMap;

			pcMap = (PageCompressHeader *)GetPageCompressMemoryMap(v->mdfd_vfd_pca, PAGE_COMPRESS_CHUNK_SIZE(reln));

			if(sync_pcmap(pcMap, WAIT_EVENT_COMPRESS_ADDRESS_FILE_SYNC) != 0)
				ereport(data_sync_elevel(ERROR),
						(errcode_for_file_access(),
							errmsg("could not msync file \"%s\": %m",
								FilePathName(v->mdfd_vfd_pca))));

			if (FileSync(v->mdfd_vfd_pcd, WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC) < 0)
				ereport(data_sync_elevel(ERROR),
						(errcode_for_file_access(),
						errmsg("could not fsync file \"%s\": %m",
								FilePathName(v->mdfd_vfd_pcd))));
		}
		else
		{
			if (FileSync(v->mdfd_vfd, WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC) < 0)
				ereport(data_sync_elevel(ERROR),
						(errcode_for_file_access(),
						errmsg("could not fsync file \"%s\": %m",
								FilePathName(v->mdfd_vfd))));
		}

		/* Close inactive segments immediately */
		if (segno > min_inactive_seg)
		{
			if(IS_COMPRESSED_MAINFORK(reln,forknum))
			{
				FileClose(v->mdfd_vfd_pca);
				FileClose(v->mdfd_vfd_pcd);
			}
			else
				FileClose(v->mdfd_vfd);
			_fdvec_resize(reln, forknum, segno - 1);
		}

		segno--;
	}
}

/*
 * register_dirty_segment() -- Mark a relation segment as needing fsync
 *
 * If there is a local pending-ops table, just make an entry in it for
 * ProcessSyncRequests to process later.  Otherwise, try to pass off the
 * fsync request to the checkpointer process.  If that fails, just do the
 * fsync locally before returning (we hope this will not happen often
 * enough to be a performance problem).
 */
static void
register_dirty_segment(SMgrRelation reln, ForkNumber forknum, MdfdVec *seg)
{
	FileTag		tag;

	INIT_MD_FILETAG(tag, reln->smgr_rnode.node, forknum, seg->mdfd_segno);

	/* Temp relations should never be fsync'd */
	Assert(!SmgrIsTemp(reln));

	if (!RegisterSyncRequest(&tag, SYNC_REQUEST, false /* retryOnError */ ))
	{
		ereport(DEBUG1,
				(errmsg("could not forward fsync request because request queue is full")));

		if(IS_COMPRESSED_MAINFORK(reln,forknum))
		{
			PageCompressHeader *pcMap;

			pcMap = (PageCompressHeader *)GetPageCompressMemoryMap(seg->mdfd_vfd_pca, PAGE_COMPRESS_CHUNK_SIZE(reln));

			if(sync_pcmap(pcMap, WAIT_EVENT_COMPRESS_ADDRESS_FILE_SYNC) != 0)
				ereport(data_sync_elevel(ERROR),
						(errcode_for_file_access(),
							errmsg("could not msync file \"%s\": %m",
								FilePathName(seg->mdfd_vfd_pca))));

			if (FileSync(seg->mdfd_vfd_pcd, WAIT_EVENT_DATA_FILE_SYNC) < 0)
				ereport(data_sync_elevel(ERROR),
						(errcode_for_file_access(),
						errmsg("could not fsync file \"%s\": %m",
								FilePathName(seg->mdfd_vfd_pcd))));
		}else
		{
			if (FileSync(seg->mdfd_vfd, WAIT_EVENT_DATA_FILE_SYNC) < 0)
				ereport(data_sync_elevel(ERROR),
						(errcode_for_file_access(),
						errmsg("could not fsync file \"%s\": %m",
								FilePathName(seg->mdfd_vfd))));
		}
	}
}

/*
 * register_unlink_segment() -- Schedule a file to be deleted after next checkpoint
 */
static void
register_unlink_segment(RelFileNodeBackend rnode, ForkNumber forknum,
						BlockNumber segno)
{
	FileTag		tag;

	INIT_MD_FILETAG(tag, rnode.node, forknum, segno);

	/* Should never be used with temp relations */
	Assert(!RelFileNodeBackendIsTemp(rnode));

	RegisterSyncRequest(&tag, SYNC_UNLINK_REQUEST, true /* retryOnError */ );
}

/*
 * register_forget_request() -- forget any fsyncs for a relation fork's segment
 */
static void
register_forget_request(RelFileNodeBackend rnode, ForkNumber forknum,
						BlockNumber segno)
{
	FileTag		tag;

	INIT_MD_FILETAG(tag, rnode.node, forknum, segno);

	RegisterSyncRequest(&tag, SYNC_FORGET_REQUEST, true /* retryOnError */ );
}

/*
 * ForgetDatabaseSyncRequests -- forget any fsyncs and unlinks for a DB
 */
void
ForgetDatabaseSyncRequests(Oid dbid)
{
	FileTag		tag;
	RelFileNode rnode;

	rnode.dbNode = dbid;
	rnode.spcNode = 0;
	rnode.relNode = 0;

	INIT_MD_FILETAG(tag, rnode, InvalidForkNumber, InvalidBlockNumber);

	RegisterSyncRequest(&tag, SYNC_FILTER_REQUEST, true /* retryOnError */ );
}

/*
 * DropRelationFiles -- drop files of all given relations
 */
void
DropRelationFiles(RelFileNode *delrels, int ndelrels, bool isRedo)
{
	SMgrRelation *srels;
	int			i;

	srels = palloc(sizeof(SMgrRelation) * ndelrels);
	for (i = 0; i < ndelrels; i++)
	{
		SMgrRelation srel = smgropen(delrels[i], InvalidBackendId);

		if (isRedo)
		{
			ForkNumber	fork;

			for (fork = 0; fork <= MAX_FORKNUM; fork++)
				XLogDropRelation(delrels[i], fork);
		}
		srels[i] = srel;
	}

	smgrdounlinkall(srels, ndelrels, isRedo);

	for (i = 0; i < ndelrels; i++)
		smgrclose(srels[i]);
	pfree(srels);
}


/*
 *	_fdvec_resize() -- Resize the fork's open segments array
 */
static void
_fdvec_resize(SMgrRelation reln,
			  ForkNumber forknum,
			  int nseg)
{
	if (nseg == 0)
	{
		if (reln->md_num_open_segs[forknum] > 0)
		{
			pfree(reln->md_seg_fds[forknum]);
			reln->md_seg_fds[forknum] = NULL;
		}
	}
	else if (reln->md_num_open_segs[forknum] == 0)
	{
		reln->md_seg_fds[forknum] =
			MemoryContextAlloc(MdCxt, sizeof(MdfdVec) * nseg);
	}
	else
	{
		/*
		 * It doesn't seem worthwhile complicating the code to amortize
		 * repalloc() calls.  Those are far faster than PathNameOpenFile() or
		 * FileClose(), and the memory context internally will sometimes avoid
		 * doing an actual reallocation.
		 */
		reln->md_seg_fds[forknum] =
			repalloc(reln->md_seg_fds[forknum],
					 sizeof(MdfdVec) * nseg);
	}

	reln->md_num_open_segs[forknum] = nseg;
}

/*
 * Return the filename for the specified segment of the relation. The
 * returned string is palloc'd.
 */
static char *
_mdfd_segpath(SMgrRelation reln, ForkNumber forknum, BlockNumber segno)
{
	char	   *path,
			   *fullpath;

	path = relpath(reln->smgr_rnode, forknum);

	if (segno > 0)
	{
		fullpath = psprintf("%s.%u", path, segno);
		pfree(path);
	}
	else
		fullpath = path;

	return fullpath;
}

/*
 * Open the specified segment of the relation,
 * and make a MdfdVec object for it.  Returns NULL on failure.
 */
static MdfdVec *
_mdfd_openseg(SMgrRelation reln, ForkNumber forknum, BlockNumber segno,
			  int oflags)
{
	MdfdVec    *v;
	File		fd,fd_pca,fd_pcd;
	char	   *fullpath,*pcfile_path;

	fullpath = _mdfd_segpath(reln, forknum, segno);

	/* open the file */
	fd = PathNameOpenFile(fullpath, O_RDWR | PG_BINARY | oflags);

	if (fd < 0)
	{
		pfree(fullpath);
		return NULL;
	}

	fd_pca = -1;
	fd_pcd = -1;
	if(IS_COMPRESSED_MAINFORK(reln,forknum))
	{
		/* open page compress address file */
		pcfile_path = psprintf("%s_pca", fullpath);
		fd_pca = PathNameOpenFile(pcfile_path, O_RDWR | PG_BINARY | oflags);

		pfree(pcfile_path);

		if (fd_pca < 0)
		{
			pfree(fullpath);
			return NULL;
		}

		/* open page compress data file */
		pcfile_path = psprintf("%s_pcd", fullpath);
		fd_pcd = PathNameOpenFile(pcfile_path, O_RDWR | PG_BINARY | oflags);

		pfree(pcfile_path);

		if (fd_pcd < 0)
		{
			pfree(fullpath);
			return NULL;
		}

		SetupPageCompressMemoryMap(fd_pca, PAGE_COMPRESS_CHUNK_SIZE(reln), PAGE_COMPRESS_ALGORITHM(reln));
	}

	pfree(fullpath);

	/*
	 * Segments are always opened in order from lowest to highest, so we must
	 * be adding a new one at the end.
	 */
	Assert(segno == reln->md_num_open_segs[forknum]);

	_fdvec_resize(reln, forknum, segno + 1);

	/* fill the entry */
	v = &reln->md_seg_fds[forknum][segno];
	v->mdfd_vfd = fd;
	v->mdfd_vfd_pca = fd_pca;
	v->mdfd_vfd_pcd = fd_pcd;
	v->mdfd_segno = segno;

	Assert(_mdnblocks(reln, forknum, v) <= ((BlockNumber) RELSEG_SIZE));

	/* all done */
	return v;
}

/*
 *	_mdfd_getseg() -- Find the segment of the relation holding the
 *		specified block.
 *
 * If the segment doesn't exist, we ereport, return NULL, or create the
 * segment, according to "behavior".  Note: skipFsync is only used in the
 * EXTENSION_CREATE case.
 */
static MdfdVec *
_mdfd_getseg(SMgrRelation reln, ForkNumber forknum, BlockNumber blkno,
			 bool skipFsync, int behavior)
{
	MdfdVec    *v;
	BlockNumber targetseg;
	BlockNumber nextsegno;

	/* some way to handle non-existent segments needs to be specified */
	Assert(behavior &
		   (EXTENSION_FAIL | EXTENSION_CREATE | EXTENSION_RETURN_NULL));

	targetseg = blkno / ((BlockNumber) RELSEG_SIZE);

	/* if an existing and opened segment, we're done */
	if (targetseg < reln->md_num_open_segs[forknum])
	{
		v = &reln->md_seg_fds[forknum][targetseg];
		return v;
	}

	/*
	 * The target segment is not yet open. Iterate over all the segments
	 * between the last opened and the target segment. This way missing
	 * segments either raise an error, or get created (according to
	 * 'behavior'). Start with either the last opened, or the first segment if
	 * none was opened before.
	 */
	if (reln->md_num_open_segs[forknum] > 0)
		v = &reln->md_seg_fds[forknum][reln->md_num_open_segs[forknum] - 1];
	else
	{
		v = mdopenfork(reln, forknum, behavior);
		if (!v)
			return NULL;		/* if behavior & EXTENSION_RETURN_NULL */
	}

	for (nextsegno = reln->md_num_open_segs[forknum];
		 nextsegno <= targetseg; nextsegno++)
	{
		BlockNumber nblocks = _mdnblocks(reln, forknum, v);
		int			flags = 0;

		Assert(nextsegno == v->mdfd_segno + 1);

		if (nblocks > ((BlockNumber) RELSEG_SIZE))
			elog(FATAL, "segment too big");

		if ((behavior & EXTENSION_CREATE) ||
			(InRecovery && (behavior & EXTENSION_CREATE_RECOVERY)))
		{
			/*
			 * Normally we will create new segments only if authorized by the
			 * caller (i.e., we are doing mdextend()).  But when doing WAL
			 * recovery, create segments anyway; this allows cases such as
			 * replaying WAL data that has a write into a high-numbered
			 * segment of a relation that was later deleted. We want to go
			 * ahead and create the segments so we can finish out the replay.
			 *
			 * We have to maintain the invariant that segments before the last
			 * active segment are of size RELSEG_SIZE; therefore, if
			 * extending, pad them out with zeroes if needed.  (This only
			 * matters if in recovery, or if the caller is extending the
			 * relation discontiguously, but that can happen in hash indexes.)
			 */
			if (nblocks < ((BlockNumber) RELSEG_SIZE))
			{
				char	   *zerobuf = palloc0(BLCKSZ);

				mdextend(reln, forknum,
						 nextsegno * ((BlockNumber) RELSEG_SIZE) - 1,
						 zerobuf, skipFsync);
				pfree(zerobuf);
			}
			flags = O_CREAT;
		}
		else if (!(behavior & EXTENSION_DONT_CHECK_SIZE) &&
				 nblocks < ((BlockNumber) RELSEG_SIZE))
		{
			/*
			 * When not extending (or explicitly including truncated
			 * segments), only open the next segment if the current one is
			 * exactly RELSEG_SIZE.  If not (this branch), either return NULL
			 * or fail.
			 */
			if (behavior & EXTENSION_RETURN_NULL)
			{
				/*
				 * Some callers discern between reasons for _mdfd_getseg()
				 * returning NULL based on errno. As there's no failing
				 * syscall involved in this case, explicitly set errno to
				 * ENOENT, as that seems the closest interpretation.
				 */
				errno = ENOENT;
				return NULL;
			}

			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\" (target block %u): previous segment is only %u blocks",
							_mdfd_segpath(reln, forknum, nextsegno),
							blkno, nblocks)));
		}

		v = _mdfd_openseg(reln, forknum, nextsegno, flags);

		if (v == NULL)
		{
			if ((behavior & EXTENSION_RETURN_NULL) &&
				FILE_POSSIBLY_DELETED(errno))
				return NULL;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\" (target block %u): %m",
							_mdfd_segpath(reln, forknum, nextsegno),
							blkno)));
		}
	}

	return v;
}

/*
 * Get number of blocks present in a single disk file
 */
static BlockNumber
_mdnblocks(SMgrRelation reln, ForkNumber forknum, MdfdVec *seg)
{
	off_t		len;
	PageCompressHeader	*pcMap;

	if(IS_COMPRESSED_MAINFORK(reln,forknum))
	{
		pcMap = (PageCompressHeader *)GetPageCompressMemoryMap(seg->mdfd_vfd_pca, PAGE_COMPRESS_CHUNK_SIZE(reln));
		return (BlockNumber) pg_atomic_read_u32(&pcMap->nblocks);
	}

	len = FileSize(seg->mdfd_vfd);
	if (len < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not seek to end of file \"%s\": %m",
						FilePathName(seg->mdfd_vfd))));
	/* note that this calculation will ignore any partial block at EOF */
	return (BlockNumber) (len / BLCKSZ);
}

/*
 * Sync a file to disk, given a file tag.  Write the path into an output
 * buffer so the caller can use it in error messages.
 *
 * Return 0 on success, -1 on failure, with errno set.
 */
int
mdsyncfiletag(const FileTag *ftag, char *path)
{
	SMgrRelation reln = smgropen(ftag->rnode, InvalidBackendId);
	File		file;
	bool		need_to_close;
	int			result,
				save_errno;

	if(IS_COMPRESSED_MAINFORK(reln,ftag->forknum))
	{
		PageCompressHeader	*pcMap;

		/* sync page compression address file */
		/* See if we already have the file open, or need to open it. */
		if (ftag->segno < reln->md_num_open_segs[ftag->forknum])
		{
			file = reln->md_seg_fds[ftag->forknum][ftag->segno].mdfd_vfd_pca;
			strlcpy(path, FilePathName(file), MAXPGPATH);
			need_to_close = false;
		}
		else
		{
			char	   *p;

			p = _mdfd_segpath(reln, ftag->forknum, ftag->segno);
			snprintf(path, MAXPGPATH, "%s_pca", p);
			pfree(p);

			file = PathNameOpenFile(path, O_RDWR | PG_BINARY);
			if (file < 0)
				return -1;

			need_to_close = true;

			SetupPageCompressMemoryMap(file, PAGE_COMPRESS_CHUNK_SIZE(reln), PAGE_COMPRESS_ALGORITHM(reln));
		}

		pcMap = (PageCompressHeader *)GetPageCompressMemoryMap(file, PAGE_COMPRESS_CHUNK_SIZE(reln));
		result = sync_pcmap(pcMap, WAIT_EVENT_COMPRESS_ADDRESS_FILE_SYNC);
		save_errno = errno;

		if (need_to_close)
			FileClose(file);

		if(result != 0)
		{
			errno = save_errno;
			return result;
		}

		/* sync page compression data file */
		/* See if we already have the file open, or need to open it. */
		if (ftag->segno < reln->md_num_open_segs[ftag->forknum])
		{
			file = reln->md_seg_fds[ftag->forknum][ftag->segno].mdfd_vfd_pcd;
			strlcpy(path, FilePathName(file), MAXPGPATH);
			need_to_close = false;
		}
		else
		{
			char	   *p;

			p = _mdfd_segpath(reln, ftag->forknum, ftag->segno);
			snprintf(path, MAXPGPATH, "%s_pcd", p);
			pfree(p);

			file = PathNameOpenFile(path, O_RDWR | PG_BINARY);
			if (file < 0)
				return -1;

			need_to_close = true;
		}

		/* Sync the page compression data file. */
		result = FileSync(file, WAIT_EVENT_DATA_FILE_SYNC);
		save_errno = errno;

		if (need_to_close)
			FileClose(file);
		
		errno = save_errno;
		return result;
	}

	/* See if we already have the file open, or need to open it. */
	if (ftag->segno < reln->md_num_open_segs[ftag->forknum])
	{
		file = reln->md_seg_fds[ftag->forknum][ftag->segno].mdfd_vfd;
		strlcpy(path, FilePathName(file), MAXPGPATH);
		need_to_close = false;
	}
	else
	{
		char	   *p;

		p = _mdfd_segpath(reln, ftag->forknum, ftag->segno);
		strlcpy(path, p, MAXPGPATH);
		pfree(p);

		file = PathNameOpenFile(path, O_RDWR | PG_BINARY);
		if (file < 0)
			return -1;
		need_to_close = true;
	}

	/* Sync the file. */
	result = FileSync(file, WAIT_EVENT_DATA_FILE_SYNC);
	save_errno = errno;

	if (need_to_close)
		FileClose(file);

	errno = save_errno;
	return result;
}

/*
 * Unlink a file, given a file tag.  Write the path into an output
 * buffer so the caller can use it in error messages.
 *
 * Return 0 on success, -1 on failure, with errno set.
 */
int
mdunlinkfiletag(const FileTag *ftag, char *path)
{
	SMgrRelation reln = smgropen(ftag->rnode, InvalidBackendId);
	char	   *p;
	int			ret;

	/* Compute the path. */
	p = relpathperm(ftag->rnode, MAIN_FORKNUM);
	strlcpy(path, p, MAXPGPATH);

	/* Try to unlink the file. */
	ret = unlink(path);

	if((ret == 0 || errno == ENOENT) &&
		IS_COMPRESSED_MAINFORK(reln,ftag->forknum))
	{
		snprintf(path, MAXPGPATH, "%s_pca", p);
		ret = unlink(path);

		if(ret == 0 || errno == ENOENT)
		{
			snprintf(path, MAXPGPATH, "%s_pcd", p);
			ret = unlink(path);
		}
	}

	pfree(p);

	return ret;
}

/*
 * Check if a given candidate request matches a given tag, when processing
 * a SYNC_FILTER_REQUEST request.  This will be called for all pending
 * requests to find out whether to forget them.
 */
bool
mdfiletagmatches(const FileTag *ftag, const FileTag *candidate)
{
	/*
	 * For now we only use filter requests as a way to drop all scheduled
	 * callbacks relating to a given database, when dropping the database.
	 * We'll return true for all candidates that have the same database OID as
	 * the ftag from the SYNC_FILTER_REQUEST request, so they're forgotten.
	 */
	return ftag->rnode.dbNode == candidate->rnode.dbNode;
}

static int
sync_pcmap(PageCompressHeader * pcMap, uint32 wait_event_info)
{
	int returnCode;
	uint32 nblocks, allocated_chunks, last_synced_nblocks, last_synced_allocated_chunks;

	nblocks = pg_atomic_read_u32(&pcMap->nblocks);
	allocated_chunks = pg_atomic_read_u32(&pcMap->allocated_chunks);
	last_synced_nblocks = pg_atomic_read_u32(&pcMap->last_synced_nblocks);
	last_synced_allocated_chunks = pg_atomic_read_u32(&pcMap->last_synced_allocated_chunks);

	pgstat_report_wait_start(wait_event_info);
	returnCode = pc_msync(pcMap);
	pgstat_report_wait_end();

	if(returnCode == 0)
	{
		if(last_synced_nblocks != nblocks)
			pg_atomic_write_u32(&pcMap->last_synced_nblocks, nblocks);

		if(last_synced_allocated_chunks != allocated_chunks)
			pg_atomic_write_u32(&pcMap->last_synced_allocated_chunks, allocated_chunks);
	}

	return returnCode;
}