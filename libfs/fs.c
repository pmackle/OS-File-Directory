#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#include "disk.h"
#include "fs.h"

#define FAT_EOC 0xFFFF

#define SIG_LEN 8

// Superblock data structure
struct __attribute__((__packed__)) superblock {
	uint8_t signature[SIG_LEN];
	uint16_t tot_amt_blks;
	uint16_t root_dir_blk_idx;
	uint16_t data_blk_start_idx;
	uint16_t amt_data_blks;
	uint8_t num_blks_fat;
	uint8_t padding[4079];
};
const uint8_t specified_signature[SIG_LEN] = {'E', 'C', 'S', '1', '5', '0', 'F', 'S'};

#define NUM_ENTRIES_FAT_BLK 2048

// FAT data structure
struct __attribute__((__packed__)) fat_block {
	uint16_t next_data_blk[NUM_ENTRIES_FAT_BLK];
};

// Root directory data structure
struct __attribute__((__packed__)) root_dir_entry {
	// Explicitly use char array for comparison to final character pointers.
	char filename[FS_FILENAME_LEN];
	uint32_t size_file;
	uint16_t idx_first_data_blk;
	uint8_t padding[10];
};

// File descriptor data structure
struct file_descriptor {
	int idx_file_root_dir;
	int file_descriptor;
	size_t file_offset;
};

// Virgin block representations, for cleaning purposes upon an unmount call.
static const struct superblock clean_superblock;
#define CLEAN_FAT NULL
static const struct root_dir_entry clean_root_dir_entry;

static struct superblock superblock;
// To be allocated once we know how many blocks are necessary.
static struct fat_block* fat;
static struct root_dir_entry root_directory[FS_FILE_MAX_COUNT];

static int num_open_fds = 0;
static const struct file_descriptor empty_FD = {
	.idx_file_root_dir = -1,
	.file_descriptor = 0,
	.file_offset = 0
};
static struct file_descriptor FD[FS_OPEN_MAX_COUNT];

// For tracking purposes.
static int fs_mounted = 0;
static int num_files_root_dir = 0;
static int num_avail_data_blks = 0;

int fs_mount(const char *diskname)
{
	if (block_disk_open(diskname)) {
		return -1;
	}

	block_read(0, &superblock);

	// Required error checks. An improper signature exists, or the provided amount of blocks does not correspond to that given by the Block API.
	if (memcmp(superblock.signature, specified_signature, SIG_LEN) || superblock.tot_amt_blks != block_disk_count()) {
		return -1;
	}

	fat = (struct fat_block*)malloc(superblock.num_blks_fat * sizeof(struct fat_block));

	// Read FAT blocks in one by one.
	int i = 1;
	while (i <= superblock.num_blks_fat) {
		block_read(i, &(fat[i - 1]));
		i++;
	}

	block_read(i, &root_directory);

	// Denote that initially, no file is associated with any of these unopened file descriptors. Since we use a non-default value (-1) to represent a lack of corresponding filename, this step is essential.
	for (int j = 0; j < FS_OPEN_MAX_COUNT; j++) {
		FD[j] = empty_FD;
	}

	fs_mounted = 1;
	// First data entry can never be written (always FAT_EOC) in FAT.
	num_avail_data_blks = superblock.amt_data_blks - 1;
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

	// Clean up our file descriptor table.
	for (int j = 0; j < FS_OPEN_MAX_COUNT; j++) {
		FD[j] = empty_FD;
	}

	fs_mounted = 0;
	num_avail_data_blks = 0;
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

static int is_invalid_file(const char* filename) {
	for (int i = 0; i <= FS_FILENAME_LEN; i++) {
		if (i == FS_FILENAME_LEN) {
			return 1;
		}

		if (filename[i] == '\0') {
			return 0;
		}
	}

	// We should never get here.
	return -1;
}

int fs_create(const char *filename)
{
	if (!fs_mounted || num_files_root_dir >= FS_FILE_MAX_COUNT || is_invalid_file(filename)) {
		return -1;
	}

	// Find the file in the root directory.
	int x;
	for (x = 0; x < FS_FILE_MAX_COUNT; x++) {
		if (!strcmp(filename, root_directory[x].filename)) {
			break;
		}
	}

	// The file already exists.
	if (x < FS_FILE_MAX_COUNT) {
		return -1;
	}

	int entry = 0;
	while (entry < FS_FILE_MAX_COUNT) {
		// Find the first empty entry in the root directory.
		if (root_directory[entry].filename[0] == '\0') {
			break;
		}

		entry++;
	}

	// Fill it in.
	int i;
	for (i = 0; i < (int)strlen(filename); i++) {
		root_directory[entry].filename[i] = filename[i];
	}
	root_directory[entry].filename[i] = '\0';
	root_directory[entry].size_file = 0;
	root_directory[entry].idx_first_data_blk = FAT_EOC;
	num_files_root_dir++;

	// Update disk.
	block_write(superblock.root_dir_blk_idx, &root_directory);

	return 0;
}

int fs_delete(const char *filename)
{
	if (!fs_mounted || is_invalid_file(filename)) {
		return -1;
	}

	int x;
	for (x = 0; x < FS_FILE_MAX_COUNT; x++) {
		if (!strcmp(filename, root_directory[x].filename)) {
			break;
		}
	}

	// The file does not exist in the file system.
	if (x >= FS_FILE_MAX_COUNT) {
		return -1;
	}

	// The file is currently open.
	for (int i = 0; i < FS_OPEN_MAX_COUNT; i++) {
		if (FD[i].idx_file_root_dir == x) {
			return -1;
		}
	}

	// No need to free space for empty files.
	if (root_directory[x].idx_first_data_blk != FAT_EOC) {
		// Find the location in the FAT that corresponds to the beginning of the file.
		int block = root_directory[x].idx_first_data_blk / NUM_ENTRIES_FAT_BLK;
		uint16_t entry = root_directory[x].idx_first_data_blk % NUM_ENTRIES_FAT_BLK;
		while (fat[block].next_data_blk[entry] != FAT_EOC) {
			uint16_t next_location = fat[block].next_data_blk[entry];
			// Delete each key to the file's contents.
			fat[block].next_data_blk[entry] = 0;
			num_avail_data_blks++;
			// Find the next FAT block with the file's data.
			block = next_location / NUM_ENTRIES_FAT_BLK;
			entry = next_location % NUM_ENTRIES_FAT_BLK;
		}
		// Free the final bytes.
		fat[block].next_data_blk[entry] = 0;
	}

	// Empty the entry in the root directory.
	root_directory[x].filename[0] = '\0';
	root_directory[x].size_file = 0;
	root_directory[x].idx_first_data_blk = FAT_EOC;

	// Write all potentially modified data back to disk.
	int i = 1;
	while (i <= superblock.num_blks_fat) {
		block_write(i, &(fat[i - 1]));
		i++;
	}
	block_write(i, &root_directory);

	return 0;
}

int fs_ls(void)
{
	if (!fs_mounted) {
		return -1;
	}

	fprintf(stdout, "FS Ls:\n");
	for (int i = 0; i < FS_FILE_MAX_COUNT; i++) {
		if (root_directory[i].filename[0] != '\0') {
			fprintf(stdout, "file: %s, size: %d, data_blk: %d\n", root_directory[i].filename, root_directory[i].size_file, root_directory[i].idx_first_data_blk);
		}
	}

	return 0;
}

int fs_open(const char *filename)
{
	if (!fs_mounted || num_open_fds >= FS_OPEN_MAX_COUNT || is_invalid_file(filename)) {
		return -1;
	}

	int i;
	for (i = 0; i < FS_FILE_MAX_COUNT; i++) {
		if (!strcmp(filename, root_directory[i].filename)) {
			break;
		}
	}

	if (i >= FS_FILE_MAX_COUNT) {
		return -1;
	}

	int fd_idx = 0;
	// This file descriptor is already open.
	while (FD[fd_idx].file_descriptor != 0) {
		fd_idx++;
	}

	FD[fd_idx].file_descriptor = fd_idx + 1;
	// Redundant, but for readability.
	FD[fd_idx].file_offset = 0;
	FD[fd_idx].idx_file_root_dir = i;

	num_open_fds++;

	return FD[fd_idx].file_descriptor;
}

int fs_close(int fd)
{
	if (!fs_mounted || fd > FS_OPEN_MAX_COUNT) {
		return -1;
	}
	
	int i;
	for (i = 0; i < FS_OPEN_MAX_COUNT; i++) {
		if (FD[i].file_descriptor == fd) {
			break;
		}
	}

	// This file descriptor is not open.
	if (i >= FS_OPEN_MAX_COUNT) {
		return -1;
	}

	// This is how we denote an unopened file descriptor.
	FD[i].file_descriptor = 0;
	FD[i].file_offset = 0;
	FD[i].idx_file_root_dir = -1;

	num_open_fds--;

	return 0;
}

int fs_stat(int fd)
{
	if (!fs_mounted || fd > FS_OPEN_MAX_COUNT) {
		return -1;
	}
	
	int i;
	for (i = 0; i < FS_OPEN_MAX_COUNT; i++) {
		if (FD[i].file_descriptor == fd) {
			break;
		}
	}

	// This file descriptor is not open.
	if (i == FS_OPEN_MAX_COUNT) {
		return -1;
	}

	return root_directory[FD[i].idx_file_root_dir].size_file;
}

int fs_lseek(int fd, size_t offset)
{
	int size_file = fs_stat(fd);
	if (size_file == -1 || (int)offset > size_file) {
		return -1;
	}
	
	int i;
	for (i = 0; i < FS_OPEN_MAX_COUNT; i++) {
		// Guaranteed to exist, for similar reasons.
		if (FD[i].file_descriptor == fd) {
			break;
		}
	}

	FD[i].file_offset = offset;

	return 0;
}

int fs_write(int fd, void *buf, size_t count)
{
	// Error checking. 
	if (!fs_mounted || fd > FS_OPEN_MAX_COUNT || buf == NULL){
		return -1;
	}

	int i;
	for (i = 0; i < FS_OPEN_MAX_COUNT; i++) {
		if (FD[i].file_descriptor == fd) {
			break;
		}
	}

	// This file descriptor is not open.
	if (i == FS_OPEN_MAX_COUNT) {
		return -1;
	}

	int x = FD[i].idx_file_root_dir;
	int j = root_directory[x].idx_first_data_blk;

	uint16_t file_blocks[superblock.amt_data_blks];

	for (int k = 0; k < superblock.amt_data_blks; k++) {
		file_blocks[k] = FAT_EOC;
	}
	// If first_data_blk is not empty
	int counter = 0;
	if (j != FAT_EOC) {
		file_blocks[0] = superblock.data_blk_start_idx + j;
	
		int block = j / NUM_ENTRIES_FAT_BLK;
		uint16_t entry = j % NUM_ENTRIES_FAT_BLK;

		counter++;
		while (fat[block].next_data_blk[entry] != FAT_EOC) {
			uint16_t next_location = fat[block].next_data_blk[entry];
			// Delete each key to the file's contents.
			file_blocks[counter] = superblock.data_blk_start_idx + next_location;
			// Find the next FAT block with the file's data.
			block = next_location / NUM_ENTRIES_FAT_BLK;
			entry = next_location % NUM_ENTRIES_FAT_BLK;
			counter++;
		}
	}
	uint8_t* file_blocks_array = counter == 0 ? NULL : (uint8_t*)malloc(counter * BLOCK_SIZE * sizeof(uint8_t));
	int t = 0;
	while (file_blocks[t] != FAT_EOC) {
		block_read(file_blocks[t], &file_blocks_array[t*BLOCK_SIZE]);
		t++;
	}

	int offset = FD[i].file_offset;
	size_t actual_count = count;
	if (count > root_directory[x].size_file - offset) {
		actual_count = root_directory[x].size_file - offset;
	}
	memcpy(&file_blocks_array[offset], buf, actual_count);

	int z = actual_count;

	// While there are still more characters to write.
	while (z < (int)count) {
		// While there is still space in the block.
		while (counter != 0 && ((z + offset) / (counter * BLOCK_SIZE) == 0)) {
			// There is nothing left to write with a block that still has space.
			if (z == (int)count){
				break;
			}
			memcpy(&file_blocks_array[z + offset], buf + z, 1);
			z++;
			(root_directory[x].size_file)++;
		}

		// While there is still more to write, but
		// we are at the end of the block.
		if (z < (int)count) {
			// If no more data blocks to spare, stop writing.
			if (num_avail_data_blks == 0) {
				break;
			}

			// Else, allocate another data block and keep going.
			else {
				uint16_t v = root_directory[x].idx_first_data_blk;

				// Must update idx_first_data_blk if empty file being written to.

				int block = v / NUM_ENTRIES_FAT_BLK;
				uint16_t entry = v % NUM_ENTRIES_FAT_BLK;
				// This loop finds the last block of the current file (0xFFFF).
				if (v != FAT_EOC) {
					while (fat[block].next_data_blk[entry] != FAT_EOC) {
						uint16_t next_location = fat[block].next_data_blk[entry];
						block = next_location / NUM_ENTRIES_FAT_BLK;
						entry = next_location % NUM_ENTRIES_FAT_BLK;
					}
				}	
				for (int c = 0; c < superblock.num_blks_fat; c++) {
					for (int d = 0; d < NUM_ENTRIES_FAT_BLK; d++) {
						if (fat[c].next_data_blk[d] == 0) {
							if (v != FAT_EOC) {
								fat[block].next_data_blk[entry] = c * NUM_ENTRIES_FAT_BLK + d;
							}
							// This is the new last block that contains the curent block.
							fat[c].next_data_blk[d] = FAT_EOC;
							file_blocks[counter] = (c * NUM_ENTRIES_FAT_BLK + d) + superblock.data_blk_start_idx;

							if (v == FAT_EOC) {
								root_directory[x].idx_first_data_blk = c * NUM_ENTRIES_FAT_BLK + d; 
							}
							// Increases the file_blocks array so we can add another block to it.
							file_blocks_array = (uint8_t*)realloc(file_blocks_array, (counter + 1) * BLOCK_SIZE * sizeof(uint8_t));

							block_read(file_blocks[counter], &file_blocks_array[counter * BLOCK_SIZE]);

							counter++;
							num_avail_data_blks--;
							break;
						}
					}
					break;
				}
			}
			
		}
	}

	int l = 0;
	while (file_blocks[l] != FAT_EOC) {
		block_write(file_blocks[l], &file_blocks_array[l * BLOCK_SIZE]);
		l++;
	}

	free(file_blocks_array);

	block_write(superblock.root_dir_blk_idx, &root_directory);

	int y = 1;
	while (y <= superblock.num_blks_fat) {
		block_write(y, &(fat[y - 1]));
		y++;
	}

	return z;
}

int fs_read(int fd, void *buf, size_t count)
{
	if (!fs_mounted || fd > FS_OPEN_MAX_COUNT || buf == NULL){
		return -1;
	}

	int i;
	for (i = 0; i < FS_OPEN_MAX_COUNT; i++) {
		if (FD[i].file_descriptor == fd) {
			break;
		}
	}

	// This file descriptor is not open.
	if (i == FS_OPEN_MAX_COUNT) {
		return -1;
	}

	int x = FD[i].idx_file_root_dir;
	int j = root_directory[x].idx_first_data_blk;

	uint16_t file_blocks[superblock.amt_data_blks];

	for (int k = 0; k < superblock.amt_data_blks; k++) {
		file_blocks[k] = FAT_EOC;
	}

	// If first_data_blk is not empty
	int counter = 0;
	if (j != FAT_EOC) {
		file_blocks[counter] = superblock.data_blk_start_idx + j;
	
		int block = j / NUM_ENTRIES_FAT_BLK;
		uint16_t entry = j % NUM_ENTRIES_FAT_BLK;
		counter++;
		while (fat[block].next_data_blk[entry] != FAT_EOC) {
			uint16_t next_location = fat[block].next_data_blk[entry];
			// Delete each key to the file's contents.
			file_blocks[counter] = superblock.data_blk_start_idx + next_location;
			// Find the next FAT block with the file's data.
			block = next_location / NUM_ENTRIES_FAT_BLK;
			entry = next_location % NUM_ENTRIES_FAT_BLK;
			counter++;
		}
	} else {
		return 0;
	}

	uint8_t file_blocks_array[counter * BLOCK_SIZE];
	int k = 0;
	while (file_blocks[k] != FAT_EOC) {
		block_read(file_blocks[k], &file_blocks_array[k*BLOCK_SIZE]);
		k++;
	}

	size_t actual_count = count;
	if (count > root_directory[x].size_file - FD[i].file_offset) {
		actual_count = root_directory[x].size_file - FD[i].file_offset;
	}
	
	memcpy(buf, &file_blocks_array[FD[i].file_offset], actual_count);

	FD[i].file_offset += actual_count;
	return actual_count;
}
