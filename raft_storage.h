#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include <fcntl.h>
#include <mutex>
#include <fstream>

template<typename command>
class raft_storage {
public:
    raft_storage(const std::string& file_dir);
    ~raft_storage();
    // Your code here
    void persistent_metadata(int voteFor,int current_term);
    void persistent_log(std::vector<log_entry<command>> &log);
    void persistent_snapshot(int lastIncludedIndex,int lastIncludedTerm,std::vector<char>& data);

    void read_metadata(int& voteFor,int& current_term);
    void read_log(std::vector<log_entry<command>> &log);
    void read_snapshot(int& lastIncludedIndex,int& lastIncludedTerm,std::vector<char>& data);
private:
    int file_size(std::ifstream& file);
    std::mutex log_mtx;
    std::mutex metadata_mtx;
    std::mutex snapshot_mtx;
    std::string file_dir;
};

template<typename command>
raft_storage<command>::raft_storage(const std::string& dir):file_dir(dir)
{
    // Your code here
    // create files if they don't exist 
    std::ofstream out_log;
    out_log.open(file_dir + "/log", std::ios::app);
    std::ofstream out_metadata;
    out_metadata.open(file_dir + "/metadata", std::ios::app);
    std::ofstream out_snapshot;
    out_snapshot.open(file_dir + "/snapshot", std::ios::app);
}

template<typename command>
raft_storage<command>::~raft_storage() {
   // Your code here
}

template<typename command>
void raft_storage<command>::persistent_metadata(int voteFor,int current_term) {
    std::lock_guard<std::mutex> grd(metadata_mtx);
    std::ofstream out_metadata;
    // trunc:rewrite the document
    // if the write disk could be interrupted, this would fail, since the committed log could be lost
    out_metadata.open(file_dir + "/metadata", std::ios::binary | std::ios::trunc);

    out_metadata.write((char *) &voteFor, sizeof(int));
    out_metadata.write((char *) &current_term, sizeof(int));
}

template<typename command>
void raft_storage<command>::persistent_log(std::vector<log_entry<command>> &log) {
    std::lock_guard<std::mutex> grd(log_mtx);
    std::ofstream out_log;
    out_log.open(file_dir + "/log", std::ios::binary | std::ios::trunc); 

    // mark how much logs
    int log_size = (int)log.size();
    out_log.write((char *)&log_size,sizeof(int));

    for (auto &it : log) {
        int size = it.cmd.size();   // cmd size may diff from each other, so can't declare outside
        char* buf = new char[size];
        it.cmd.serialize(buf, size);
        
        out_log.write((char *) &it.index, sizeof(int));
        out_log.write((char *) &it.term, sizeof(int));
        out_log.write((char *) &size, sizeof(int));
        out_log.write(buf, size);
        delete [] buf;
    }
}

template<typename command>
void raft_storage<command>::persistent_snapshot(int lastIncludedIndex,int lastIncludedTerm,std::vector<char>& data){
    std::unique_lock<std::mutex> grd_snapshot(snapshot_mtx);
    std::ofstream out_snapshot;
    out_snapshot.open(file_dir + "/snapshot", std::ios::binary | std::ios::trunc);

    out_snapshot.write((char *) &lastIncludedIndex, sizeof(int));
    out_snapshot.write((char *) &lastIncludedTerm, sizeof(int));
    out_snapshot.write((char *) data.data(), data.size());
}

template<typename command>
void raft_storage<command>::read_metadata(int& voteFor,int& current_term){
    std::unique_lock<std::mutex> grd_metadata(metadata_mtx);
    std::ifstream in_metadata;
    in_metadata.open(file_dir +  "/metadata" , std::ios::binary);

    if(file_size(in_metadata) == 0){
        voteFor = -1;
        current_term = 0;
    }else{
        in_metadata.read((char *) &voteFor, sizeof(int));
        in_metadata.read((char *) &current_term, sizeof(int));
    }
}

template<typename command>
void raft_storage<command>::read_log(std::vector<log_entry<command>> &log){
    std::unique_lock<std::mutex> grd_log(log_mtx);
    std::ifstream in_log;
    in_log.open(file_dir + "/log" , std::ios::binary);

    log.resize(0);
    if(file_size(in_log) == 0){
        log_entry<command> new_entry;
        new_entry.index = 0;
        new_entry.term = 0;
        log.push_back(new_entry);
        return;
    }

    int log_size;
    in_log.read((char*)&log_size,sizeof(int));
    for(int i = 0 ; i < log_size ; i++)
    {
        log_entry<command> new_entry;
        int size;
        char *buf;
        in_log.read((char *) &new_entry.index, sizeof(int));
        in_log.read((char *) &new_entry.term, sizeof(int));
        in_log.read((char *) &size, sizeof(int));
        buf = new char [size];
        in_log.read(buf, size);
        new_entry.cmd.deserialize(buf, size);
        log.push_back(new_entry);
        delete [] buf;
    }
}

template<typename command>
void raft_storage<command>::read_snapshot(int& lastIncludedIndex,int& lastIncludedTerm,std::vector<char>& data){
    std::unique_lock<std::mutex> grd_snapshot(snapshot_mtx);
    std::ifstream in_snapshot;
    in_snapshot.open(file_dir + "/snapshot" , std::ios::binary);

    int size = file_size(in_snapshot);
    if(size == 0){
        lastIncludedIndex = 0;
        lastIncludedIndex = 0;
        return;
    }

    int data_size = size - 2*sizeof(int);
    data.resize(data_size);
    in_snapshot.read((char *) &lastIncludedIndex,sizeof(int));
    in_snapshot.read((char *) &lastIncludedTerm,sizeof(int));
    in_snapshot.read((char *) data.data(),data_size);
} 

template<typename command>
int raft_storage<command>::file_size(std::ifstream &file){
    file.seekg(0,std::ios::end);
    std::streampos pos = file.tellg();
    file.seekg(0,std::ios::beg);
    return (int)pos;
}
#endif // raft_storage_h