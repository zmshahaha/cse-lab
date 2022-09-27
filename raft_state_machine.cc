#include "raft_state_machine.h"

#define ALIGN(x) (((x)+3)/4*4)
kv_command::kv_command() : kv_command(CMD_NONE, "", "") { }

kv_command::kv_command(command_type tp, const std::string &key, const std::string &value) : 
    cmd_tp(tp), key(key), value(value), res(std::make_shared<result>())
{
    res->start = std::chrono::system_clock::now();
    res->key = key;
}

kv_command::kv_command(const kv_command &cmd) :
    cmd_tp(cmd.cmd_tp), key(cmd.key), value(cmd.value), res(cmd.res) {}

kv_command::~kv_command() { }

int kv_command::size() const {
    // Your code here:
    return (int)sizeof(command_type)+ALIGN((int)key.size())+ALIGN((int)value.size())+2*(int)sizeof(int);
}


void kv_command::serialize(char* buf, int sz) const {
    // Your code here:
    if(sz !=size())return;

    memcpy(buf,&cmd_tp,sizeof(command_type));
    int key_size = (int)key.size();
    int val_size = (int)value.size();
    memcpy(buf+sizeof(command_type),&key_size,sizeof(int));
    memcpy(buf+sizeof(command_type)+sizeof(int),key.c_str(),key_size);
    memcpy(buf+sizeof(command_type)+sizeof(int)+ALIGN(key_size),&val_size,sizeof(int));
    memcpy(buf+sizeof(command_type)+2*sizeof(int)+ALIGN(key_size),value.c_str(),val_size);
}

void kv_command::deserialize(const char* buf, int sz) {
    // Your code here:
    int key_size = *(int*)(buf + sizeof(command_type));
    int val_size = *(int*)(buf + sizeof(command_type) + sizeof(int)+ALIGN(key_size));

    if(sz != (int)sizeof(command_type)+2*(int)sizeof(int)+ALIGN(key_size)+ALIGN(val_size)) return;   // if call size() return 12(str isn't inited) 
    
    cmd_tp = *(command_type*)buf;
    const char* key_c_str = buf + sizeof(command_type) + sizeof(int);
    key.assign(key_c_str,key_size);
    const char* val_c_str = buf + sizeof(command_type) + 2*sizeof(int) + ALIGN(key_size);
    value.assign(val_c_str,val_size);
}

marshall& operator<<(marshall &m, const kv_command& cmd) {
    // Your code here:
    m<<cmd.cmd_tp<<cmd.key<<cmd.value;
    return m;
}

unmarshall& operator>>(unmarshall &u, kv_command& cmd) {
    // Your code here:
    u>>(int&)cmd.cmd_tp>>cmd.key>>cmd.value;
    return u;
}

kv_state_machine::~kv_state_machine() {

}

void kv_state_machine::apply_log(raft_command &cmd) {
    kv_command &kv_cmd = dynamic_cast<kv_command&>(cmd);
    std::unique_lock<std::mutex> lock(kv_cmd.res->mtx);
    // Your code here:
    kv_cmd.res->key = kv_cmd.key;
    auto result = kv_store.find(kv_cmd.key);

    switch(kv_cmd.cmd_tp)
    {
    case kv_command::command_type::CMD_NONE:
        kv_cmd.res->succ = true;
        break;
    case kv_command::command_type::CMD_GET:
        if(result == kv_store.end()){
            kv_cmd.res->succ=false;
        }else{
            kv_cmd.res->succ=true;
            kv_cmd.res->value = result -> second;
        }
        break;
    case kv_command::command_type::CMD_PUT:
        if(result == kv_store.end()){
            kv_cmd.res->succ = true;
            kv_store.insert(std::make_pair(kv_cmd.key,kv_cmd.value));
            kv_cmd.res->value = kv_cmd.value;
        }else{
            kv_cmd.res->succ = false;
            kv_store[kv_cmd.key]=kv_cmd.value;
            kv_cmd.res->value = result -> second;   
        }
        break;
    case kv_command::command_type::CMD_DEL:
        if(result == kv_store.end()){
            kv_cmd.res->succ = false;
        }else{
            kv_cmd.res->succ = true;
            kv_cmd.res->value = result -> second;
            kv_store.erase(result);
        }
        break;
    default:
        kv_cmd.res->succ = false;
    }

    kv_cmd.res->done = true;
    kv_cmd.res->cv.notify_all();
    return;
}

std::vector<char> kv_state_machine::snapshot() {
    // Your code here:
    std::vector<char> snapshot;

    for(auto it : kv_store){
        int key_size = (int)it.first.size();
        int val_size = (int)it.second.size();
        int size = 2*sizeof(int)+ALIGN(key_size)+ALIGN(val_size);
        int prev_size = (int)snapshot.size();
        snapshot.resize(prev_size+size);
        char* mem = snapshot.data()+prev_size;

        memcpy(mem,&key_size,sizeof(int));
        memcpy(mem+sizeof(int),it.first.c_str(),key_size);
        memcpy(mem+sizeof(int)+ALIGN(key_size),&val_size,sizeof(int));
        memcpy(mem+2*sizeof(int)+ALIGN(key_size),it.second.c_str(),val_size);
    }
    return snapshot;
}

void kv_state_machine::apply_snapshot(const std::vector<char>& snapshot) {
    // Your code here:
    const char* mem = snapshot.data();
    int size = (int)snapshot.size();
    kv_store.clear();

    for(int it = 0 ; it < size ; ){
        std::string key,value;

        int key_size = *(int*)(mem+it);
        it+=sizeof(int);
        key.assign(mem+it,key_size);
        it+=ALIGN(key_size);
        int val_size = *(int*)(mem+it);
        it+=sizeof(int);
        value.assign(mem+it,val_size);
        it+=ALIGN(val_size);

        kv_store.insert(std::make_pair(key,value));
    }
}
