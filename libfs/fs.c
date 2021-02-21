#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#include "disk.h"
#include "fs.h"

#define SIG_LEN 8
#define SUP_BLK_PAD 4079

struct __attribute__((__packed__)) superblock {
	uint8_t signature[SIG_LEN];
	uint16_t tot_amt_blks;
	uint16_t root_dir_blk_idx;
	uint16_t data_blk_start_idx;
	uint16_t amt_data_blks;
	uint8_t num_blks_fat;
	uint8_t padding[SUP_BLK_PAD];
};
const uint8_t specified_signature[SIG_LEN] = {'E', 'C', 'S', '1', '5', '0', 'F', 'S'};

#define NUM_ENTRIES_FAT_BLK 2048

struct __attribute__((__packed__)) fat_block {
	uint16_t next_data_blk[NUM_ENTRIES_FAT_BLK];
};

#define ROOT_DIR_PAD 10

struct __attribute__((__packed__)) root_dir_entry {
	uint8_t filename[FS_FILENAME_LEN];
	uint32_t size_file;
	uint16_t idx_first_data_blk;
	uint8_t padding[ROOT_DIR_PAD];
};

// Virgin block representations, for cleaning purposes upon an unmount call.
static const struct superblock clean_superblock;
#define CLEAN_FAT NULL
static const struct root_dir_entry clean_root_dir_entry;

// For tracking purposes.
static int fs_mounted = 0;
static int num_open_fds = 0;

static struct superblock superblock;
// To be allocated once we know how many blocks are necessary.
static struct fat_block* fat;
static struct root_dir_entry root_directory[FS_FILE_MAX_COUNT];

#define SUP_BLK_IDX 0

int fs_mount(const char *diskname)
{
	if (block_disk_open(diskname)) {
		return -1;
	}

	block_read(SUP_BLK_IDX, &superblock);

	// Required error checks. An improper signature exists, or the provided amount of blocks does not correspond to that given by the Block API.
	if (memcmp(superblock.signature, specified_signature, SIG_LEN) || superblock.tot_amt_blks != block_disk_count()) {
		return -1;
	}

	fat = (struct fat_block*)malloc(superblock.num_blks_fat * sizeof(struct fat_block));

	// Read FAT blocks in one by one.
	int i = SUP_BLK_IDX + 1;
	while (i <= superblock.num_blks_fat) {
		block_read(i, &(fat[i - 1]));
		i++;
	}

	block_read(i, &root_directory);

	fs_mounted = 1;
	return 0;
}

int fs_umount(void)
{
	if (!fs_mounted || num_open_fds > 0 || block_disk_close()) {
		return -1;
	}

	// Clean up our metadata blocks.
	superblock = clean_superblock;
	free(fat);
	fat = CLEAN_FAT;
	// We have to reset our root directory entry by entry, due to its implementation's static nature.
	for (int i = 0; i < FS_FILE_MAX_COUNT; i++) {
		root_directory[i] = clean_root_dir_entry;
	}

	fs_mounted = 0;
	return 0;
}

int fs_info(void)
{
	if (block_disk_count() < 0) {
		return -1;
	}

	fprintf(stdout, "FS Info:\n");
	fprintf(stdout, "total_blk_count=%d\n", superblock.tot_amt_blks);
	fprintf(stdout, "fat_blk_count=%d\n", superblock.num_blks_fat);
	fprintf(stdout, "rdir_blk=%d\n", 1 + superblock.num_blks_fat);
	fprintf(stdout, "data_blk=%d\n", 1 + superblock.num_blks_fat + 1);
	fprintf(stdout, "data_blk_count=%d\n", superblock.amt_data_blks);

	int data_blk_ctr = 0;
	int num_free_data_blks = 0;
	for (int i = 0; i < superblock.num_blks_fat; i++) {
		for (int j = 0; j < NUM_ENTRIES_FAT_BLK; j++) {
			// Don't count extra bytes unused by the FAT.
			if (data_blk_ctr >= superblock.amt_data_blks) {
				break;
			}

			if (fat[i].next_data_blk[j] == 0) {
				num_free_data_blks++;
			}

			data_blk_ctr++;
		}

		// Seemingly redundant but necessary, to avoid use of goto.
		if (data_blk_ctr >= superblock.amt_data_blks) {
			break;
		}
	}

	fprintf(stdout, "fat_free_ratio=%d/%d\n", num_free_data_blks, superblock.amt_data_blks);

	// Recycling this variable.
	num_free_data_blks = 0;
	for (int i = 0; i < FS_FILE_MAX_COUNT; i++) {
		if (root_directory[i].filename[0] == '\0') {
			num_free_data_blks++;
		}
	}

	fprintf(stdout, "rdir_free_ratio=%d/%d\n", num_free_data_blks, FS_FILE_MAX_COUNT);

	return 0;
}

int fs_create(const char *filename)
{
	// TODO.
	(void)filename;
	return -1;
}

int fs_delete(const char *filename)
{
	// TODO.
	(void)filename;
	return -1;
}

int fs_ls(void)
{
	// TODO.
	return -1;
}

int fs_open(const char *filename)
{
	// TODO.
	(void)filename;
	return -1;
}

int fs_close(int fd)
{
	// TODO.
	(void)fd;
	return -1;
}

int fs_stat(int fd)
{
	// TODO.
	(void)fd;
	return -1;
}

int fs_lseek(int fd, size_t offset)
{
	// TODO.
	(void)fd;
	(void)offset;
	return -1;
}

int fs_write(int fd, void *buf, size_t count)
{
	// TODO.
	(void)fd;
	(void)buf;
	(void)count;
	return -1;
}

int fs_read(int fd, void *buf, size_t count)
{
	// TODO.
	(void)fd;
	(void)buf;
	(void)count;
	return -1;
}
