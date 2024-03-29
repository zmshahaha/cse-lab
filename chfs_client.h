#ifndef chfs_client_h
#define chfs_client_h

#include <string>
//#include "chfs_protocol.h"
#include "extent_client.h"
#include <vector>

#define MAX_FILE_NAME 100

class chfs_client {
  extent_client *ec;
 public:

  typedef unsigned long long inum;
  enum xxstatus { OK, RPCERR, NOENT, IOERR, EXIST };
  typedef int status;

  struct fileinfo {
    unsigned long long size;
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirinfo {
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  typedef struct dirent {
    char name[MAX_FILE_NAME];
    //std::string name;
    chfs_client::inum inum;
  }dir_t;

 private:
  static std::string filename(inum);
  static inum n2i(std::string);
  int append_parent_d(inum,const char *,inum);

 public:
  chfs_client(std::string);
  chfs_client(std::string, std::string);

  bool isfile(inum);
  bool isdir(inum);
  bool issymlink(inum inum);

  int getfile(inum, fileinfo &);
  int getdir(inum, dirinfo &);

  int setattr(inum, size_t);
  int lookup(inum, const char *, inum &);
  int create(inum, const char *, mode_t, inum &);
  int readdir(inum, std::list<dirent> &);
  int write(inum, size_t, off_t, const char *, size_t &);
  int read(inum, size_t, off_t, std::string &);
  int unlink(inum,const char *);
  int mkdir(inum , const char *, mode_t , inum &);
  int mksymlink(inum , const char *, const char *, inum &);
  int readlink(inum , std::string &);
  
  /** you may need to add symbolic link related methods here.*/
};

#endif 
