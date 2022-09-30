#include "ch_db.h"

int view_server::execute(unsigned int query_key, unsigned int proc, const chdb_protocol::operation_var &var, int &r) {
    // TODO: Your code here
    if (proc == chdb_protocol::Dummy || proc == chdb_protocol::Get || proc == chdb_protocol::Put) {
        chdb_command::command_type cmd_type;
        switch (proc) {
        case chdb_protocol::rpc_numbers::Dummy:
            cmd_type = chdb_command::command_type::CMD_NONE;
            break;
        case chdb_protocol::rpc_numbers::Put:
            cmd_type = chdb_command::command_type::CMD_PUT;
            break;
        case chdb_protocol::rpc_numbers::Get:
            cmd_type = chdb_command::command_type::CMD_GET;
            break;
        default:
            break;
        }

        chdb_command cmd(cmd_type, var.key, var.value, var.tx_id);
        int term, index;
        
        int wait_times = 0;
        while (!cmd.res->done){
            if(wait_times%10==0){
                this->leader()->new_command(cmd, term, index);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            wait_times++;
        }
            
    }
    int base_port = this->node->port();
    int shard_offset = this->dispatch(query_key, shard_num());

    return this->node->template call(base_port + shard_offset, proc, var, r);
}

view_server::~view_server() {
#if RAFT_GROUP
    delete this->raft_group;
#endif
    delete this->node;

}