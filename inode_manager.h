// inode layer interface.

#ifndef inode_h
#define inode_h

#include <stdint.h>
#include "extent_protocol.h" // TODO: delete it

#define DISK_SIZE  1024*1024*16
#define BLOCK_SIZE 512
#define BLOCK_NUM  (DISK_SIZE/BLOCK_SIZE)

typedef uint32_t blockid_t;

class disk;
class block_manager;
class inode_manager;
// disk layer -----------------------------------------

class disk {
 private:
  unsigned char blocks[BLOCK_NUM][BLOCK_SIZE];

 public:
  disk();
  void read_block(uint32_t id, char *buf);
  void write_block(uint32_t id, const char *buf);
};

// block layer -----------------------------------------

typedef struct superblock {
  uint32_t size;
  uint32_t nblocks;
  uint32_t ninodes;
} superblock_t;

class block_manager {
 private:
  disk *d;
  std::map <uint32_t, int> using_blocks;
  //bool using_blocks[BLOCK_NUM];
 public:
  block_manager();
  superblock_t sb;

  uint32_t alloc_block();
  void free_block(uint32_t id);
  void read_block(uint32_t id, char *buf);
  void write_block(uint32_t id, const char *buf);
};

// inode layer -----------------------------------------
/*
 *disk model
 *   |.........0.........|.........1..........|...............|
 *   |boot block(ignored)|super block(ignored)|bitmap of block|
 * 
 *   |...........|.....|
 *   |inode table|block|
 */

#define INODE_NUM  1024

// Inodes per block. (in inode table)
#define IPB           (BLOCK_SIZE / sizeof(struct inode))
//1

// Block id containing inode inum i
#define IBLOCK(i)     ((BLOCK_NUM+BPB-1)/BPB + (i-1)/IPB + 2)
//2:boot block+super block
//(BLOCK_NUM+BPB-1)/BPB:bitmap of blocks (rounded up)
//i/IPB:inode block (rounded down)

#define METABLOCK     IBLOCK(INODE_NUM)//last meta block id

// Bitmap bits per block (=block size by bit)
#define BPB           (BLOCK_SIZE*8)

// Block containing bit for block b(in bitmap)
#define BBLOCK(b) ((b)/BPB + 2)

#define NDIRECT 100
#define NINDIRECT (BLOCK_SIZE / sizeof(uint))
#define MAXFILE (NDIRECT + NINDIRECT)

typedef struct inode {
  short type;
  unsigned int size;
  unsigned int atime;
  unsigned int mtime;
  unsigned int ctime;
  blockid_t blocks[NDIRECT+1];   // Data block addresses
} inode_t;

class inode_manager {
 private:
  block_manager *bm;
  struct inode* get_inode(uint32_t inum);
  void put_inode(uint32_t inum, struct inode *ino);
  blockid_t findNthBolckNum(inode* inode_, uint32_t nth);
  void allocNthBolck(inode* inode_, uint32_t nth);
  void freeNthBolck(inode* inode_, uint32_t nth);
  
 public:
  inode_manager();
  uint32_t alloc_inode(uint32_t type);
  void free_inode(uint32_t inum);
  void read_file(uint32_t inum, char **buf, int *size);
  void write_file(uint32_t inum, const char *buf, int size);
  void remove_file(uint32_t inum);
  void getattr(uint32_t inum, extent_protocol::attr &a);
};

#endif

