#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
  if(id<0||id>BLOCK_NUM)return;//exception control???
  memcpy(buf,blocks[id],BLOCK_SIZE);
}

void
disk::write_block(blockid_t id, const char *buf)
{
  if(id<0||id>BLOCK_NUM)return;//exception control???
  memcpy(blocks[id],buf,BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
  for(blockid_t i=METABLOCK+1;i<BLOCK_NUM;i++)
    if(!using_blocks[i]){
      using_blocks[i]=1;
      return i;
    }
  exit(1);//exception??
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  using_blocks[id]=0;
  return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your code goes here.
   * note: the normal inode block should begin from the 1nd inode block.
   * the 0st is used for root_dir, see inode_manager::inode_manager().
   */
  for(unsigned int inum=1;inum<=INODE_NUM;inum++)
    if(!get_inode(inum)){
      inode* inodeToUse=new inode();
      inodeToUse->size=0;
      inodeToUse->type=type;
      inodeToUse->atime=time(0);
      inodeToUse->ctime=time(0);
      inodeToUse->mtime=time(0);
      put_inode(inum,inodeToUse);
      free(inodeToUse);
      return inum;
    }
  exit(2);
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
  inode* inode_=get_inode(inum);
  if(!inode_)return;
  inode_->size=0;
  inode_->type=0;
  put_inode(inum,inode_);
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
/* inum begin with 0 end with INODE_NUM-1 */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  if (inum < 1 || inum > INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum), buf);

  ino_disk = (struct inode*)buf + (inum-1)%IPB;
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum), buf);
  ino_disk = (struct inode*)buf + (inum-1)%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_out
   */
  inode* inode_=get_inode(inum);
  if(!inode_)return;

  //copy content to buf by block
  *size=inode_->size;
  int blockNum=(*size-1)/BLOCK_SIZE+1;
  char* buf=(char*)malloc(BLOCK_SIZE*blockNum);
  for(int i=0;i<blockNum;i++){
    int blockth=findNthBolckNum(inode_,i);
    bm->read_block(blockth,buf+BLOCK_SIZE*i);
  }
  *buf_out=buf;

  //change inode
  inode_->atime=time(0);
  inode_->ctime=time(0);
  put_inode(inum,inode_);
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */
  inode* inode_=get_inode(inum);
  if(!inode_) return;
  int blockNum=(size-1)/BLOCK_SIZE+1;
  int previousBlockNum=(inode_->size>0)?(inode_->size-1)/BLOCK_SIZE+1:0;

  char temp_buf[BLOCK_SIZE*blockNum];
  memcpy(temp_buf,buf,size);

  //change blocknum
  if(blockNum<previousBlockNum)
    for(int i=previousBlockNum-1;i>=blockNum;i--)
      freeNthBolck(inode_,i);
  else if(blockNum>previousBlockNum)
    for(int i=previousBlockNum;i<blockNum;i++)
      allocNthBolck(inode_,i);

  //change content
  for(int i=0;i<blockNum;i++){
    blockid_t block_number=findNthBolckNum(inode_,i);
    bm->write_block(block_number,temp_buf+BLOCK_SIZE*i);
  }

  //change inode
  inode_->size=size;
  inode_->mtime=time(0);
  inode_->ctime=time(0);
  inode_->atime=time(0);
  put_inode(inum,inode_);
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  inode* inode_=get_inode(inum);
  if(!inode_)return;
  a.atime=inode_->atime;
  a.ctime=inode_->ctime;
  a.mtime=inode_->mtime;
  a.size=inode_->size;
  a.type=inode_->type;
  free(inode_);
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
  inode* inode_=get_inode(inum);
  if(!inode_)return;
  int blockNum=(inode_->size>0)?(inode_->size-1)/BLOCK_SIZE+1:0;
  for(int i=0;i<blockNum;i++){
    int blockth=findNthBolckNum(inode_,i);
    bm->free_block(blockth);
  }
  free_inode(inum);  
  return;
}

blockid_t inode_manager::findNthBolckNum(inode* inode_, uint32_t nth){
  if(nth<NDIRECT)
    return inode_->blocks[nth];
  char buf[BLOCK_SIZE];
  bm->read_block(inode_->blocks[NDIRECT],buf);
  return ((blockid_t*)buf)[nth-NDIRECT];
}

void inode_manager::allocNthBolck(inode* inode_, uint32_t nth){
  if(nth<NDIRECT){
    inode_->blocks[nth] = bm->alloc_block();
    return;
  }
  if(nth==NDIRECT){
    int indirectBlockNum=bm->alloc_block();
    inode_->blocks[NDIRECT]=indirectBlockNum;
  }
  
  int blockNum=bm->alloc_block();
  char buf[BLOCK_SIZE];
  bm->read_block(inode_->blocks[NDIRECT],buf);
  ((blockid_t*)buf)[nth-NDIRECT]=blockNum;
  bm->write_block(inode_->blocks[NDIRECT],buf);
}

void inode_manager::freeNthBolck(inode* inode_, uint32_t nth){
  blockid_t blockNum=findNthBolckNum(inode_,nth);
  bm->free_block(blockNum);
  if(nth==NDIRECT)
    bm->free_block(inode_->blocks[NDIRECT]);
}