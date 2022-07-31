// chfs client.  implements FS operations using extent and lock server
#include "chfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

chfs_client::chfs_client()
{
    ec = new extent_client();
}

chfs_client::chfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client();
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

//convert string to num
chfs_client::inum
chfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

//view inum as filename
std::string
chfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
chfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    printf("isfile: %lld is not a file\n", inum);
    return false;
}
/** 
 * Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * */

bool
chfs_client::isdir(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_DIR) {
        printf("isfile: %lld is a dir\n", inum);
        return true;
    } 
    printf("isfile: %lld is not a dir\n", inum);
    return false;
}

bool
chfs_client::issymlink(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_SYMLINK) {
        printf("isfile: %lld is a slink\n", inum);
        return true;
    } 
    printf("isfile: %lld is not a slink\n", inum);
    return false;
}

int 
chfs_client::readlink(inum inum, std::string &link)
{   
    if(ec->get(inum,link)!=OK)
        return IOERR;
    return OK;
}

int
chfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
chfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int
chfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    std::string buf;

    if (ec->get(ino,buf)!= extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    
    buf.resize(size,'\0');

    if (ec->put(ino,buf)!= extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

release:
    return r;
}

int
chfs_client::append_parent_d(inum parent,const char *name,inum inum_son)
{
    std::string buf;
    dir_t item;

    if(ec->get(parent,buf)!=OK)
        return IOERR;
    item.inum=inum_son;
    strcpy(item.name,name);
    buf.append((char*)(&item),sizeof(dir_t));
    if(ec->put(parent,buf)!=OK)
        return IOERR;

    return OK;
}
//what is mode????
int
chfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, ***you must remember to modify the parent infomation***.
     */
    if(lookup(parent,name,ino_out)==EXIST){
        printf("creat dup name\n");
        return IOERR;
    }

    if(ec->create(extent_protocol::T_FILE,ino_out)!=OK)
        return IOERR;
    
    return append_parent_d(parent,name,ino_out);
}

int
chfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */ 
    if(lookup(parent,name,ino_out)==EXIST){
        printf("creat dup name\n");
        return IOERR;
    }
    
    if(ec->create(extent_protocol::T_DIR,ino_out)!=OK)
        return IOERR;
    
    return append_parent_d(parent,name,ino_out);
}

int
chfs_client::mksymlink(inum parent, const char *name, const char *link, inum &ino_out)
{
    if(lookup(parent,name,ino_out)==EXIST){
        printf("creat dup name\n");
        return IOERR;
    }
    
    if(ec->create(extent_protocol::T_SYMLINK,ino_out)!=OK)
        return IOERR;

    //content only link string
    if(ec->put(ino_out,std::string(link))!=OK)
        return IOERR;

    return append_parent_d(parent,name,ino_out);
}

int
chfs_client::lookup(inum parent, const char *name, inum &ino_out)
{
    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    std::list<dirent> list;

    readdir(parent,list);
    
    for(dir_t item:list)
        if(strcmp(item.name,name)==0){
            ino_out=item.inum;
            return EXIST;
        }

    return NOENT;
}

int
chfs_client::readdir(inum dir, std::list<dir_t> &list)
{
    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    std::string buf;

    if(ec->get(dir,buf)!=OK)
    {
        return IOERR;
    }

    int dir_size=buf.size();
    int dir_t_size=sizeof(dir_t);
    const char* ptr=buf.c_str();
    for(uint32_t i=0;i<(uint32_t)dir_size/dir_t_size;i++){
        dir_t tmp_dirent;
        memcpy(&tmp_dirent,ptr+i*dir_t_size,dir_t_size);
        list.push_back(tmp_dirent);
    }

    return OK;
}

int
chfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    /*
     * your code goes here.
     * note: read using ec->get().
     */
    std::string buf;

    if(ec->get(ino,buf)!=OK)
        return IOERR;
    if(off>=(long)buf.size())
        return OK;
    data=buf.substr(off,size);

    return OK;
}

int
chfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
    std::string buf;
    std::string writeStr;

    if(ec->get(ino,buf)!=OK)
        return IOERR;
    writeStr.assign(data,size);
    if(off>=(long)buf.size())
        buf.resize(off+size,'\0');
    bytes_written=size;
    buf.replace(off,size,writeStr);
    if(ec->put(ino,buf)!=OK)
        return IOERR;

    return OK;
}

int chfs_client::unlink(inum parent,const char *name)
{
    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
    inum ino_out;
    std::string buf;
    dir_t del_dir;
    
    if(lookup(parent,name,ino_out)==NOENT)
        return NOENT;
    if(ec->remove(ino_out)!=OK)
        return IOERR;

    //remove parent entry
    if(ec->get(parent,buf)!=OK)
        return IOERR;
    int dir_size=buf.size();
    int dir_t_size=sizeof(dir_t);
    const char* ptr=buf.c_str();
    for(uint32_t i=0;i<(uint32_t)dir_size/dir_t_size;i++){
        memcpy(&del_dir,ptr+i*dir_t_size,dir_t_size);
        if(del_dir.inum==ino_out)
        {
            buf.erase(i*dir_t_size,dir_t_size);
            break;
        }    
    }
    if(ec->put(parent,buf)!=OK)
        return IOERR;

    return OK;
}

