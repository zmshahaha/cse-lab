#include "chdb_state_machine.h"

chdb_command::chdb_command() 
        :chdb_command(CMD_NONE, 0, 0, 0){
    // TODO: Your code here
    // init by below method, have res init
}

chdb_command::chdb_command(command_type tp, const int &key, const int &value, const int &tx_id)
        : cmd_tp(tp), key(key), value(value), tx_id(tx_id), res(std::make_shared<result>()) {
    // TODO: Your code here
    res->start = std::chrono::system_clock::now();
    res->key = key;
    res->tx_id = tx_id;
    res->done = false;
    res->tp = cmd_tp;
}

chdb_command::chdb_command(const chdb_command &cmd) :
        cmd_tp(cmd.cmd_tp), key(cmd.key), value(cmd.value), tx_id(cmd.tx_id), res(cmd.res){
    // TODO: Your code here
}


void chdb_command::serialize(char *buf, int sz) const {
    // TODO: Your code here
    if(sz != size())return;

    memcpy(buf,&cmd_tp,sizeof(command_type));
    memcpy(buf+sizeof(command_type),&key,sizeof(int));
    memcpy(buf+sizeof(command_type)+sizeof(int),&value,sizeof(int));
    memcpy(buf+sizeof(command_type)+2*sizeof(int),&tx_id,sizeof(int));
}

void chdb_command::deserialize(const char *buf, int sz) {
    // TODO: Your code here
    if(sz != size())return;
    
    cmd_tp = *(command_type*)buf;
    key = *(int*)(buf + sizeof(command_type));
    value = *(int*)(buf + sizeof(command_type) + sizeof(int));
    tx_id = *(int*)(buf + sizeof(command_type) + 2 * sizeof(int));
}

marshall &operator<<(marshall &m, const chdb_command &cmd) {
    // TODO: Your code here
    m<<(int)(cmd.cmd_tp)<<cmd.key<<cmd.value<<cmd.tx_id;
    return m;
}

unmarshall &operator>>(unmarshall &u, chdb_command &cmd) {
    // TODO: Your code here
    int command;
    u >>command >>cmd.key>>cmd.value>>cmd.tx_id;
    cmd.cmd_tp = (chdb_command::command_type)command;
    return u;
}

void chdb_state_machine::apply_log(raft_command &cmd) {
    // TODO: Your code here
    chdb_command &db_cmd = dynamic_cast<chdb_command &>(cmd);
    std::unique_lock<std::mutex> lock(db_cmd.res->mtx);
    
    switch (db_cmd.cmd_tp) {
    case chdb_command::command_type::CMD_NONE:
        break;
    case chdb_command::command_type::CMD_GET:
        if (kv_table.find(db_cmd.key) == kv_table.end()) {
            db_cmd.res->value = 0;
        } else {
            db_cmd.res->value = kv_table.find(db_cmd.key)->second;
        }
        break;
    case chdb_command::command_type::CMD_PUT:
        kv_table[db_cmd.key] = db_cmd.value;
        db_cmd.res->value = db_cmd.value;
        break;
    default:
        break;
    }
    db_cmd.res->done = true;
    db_cmd.res->cv.notify_all();
    return;
}