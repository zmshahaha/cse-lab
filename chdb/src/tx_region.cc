#include "tx_region.h"


int tx_region::put(const int key, const int val) {
    // TODO: Your code here
    int r;
    this->db->vserver->execute(key, 
                               chdb_protocol::Put,
                               chdb_protocol::operation_var{.tx_id = tx_id, .key = key, .value = val},
                               r);
    query_keys_write.push_back(key);
    return r;
}

int tx_region::get(const int key) {
    // TODO: Your code here
    int r;
    this->db->vserver->execute(key,
                               chdb_protocol::Get,
                               chdb_protocol::operation_var{.tx_id = tx_id, .key = key, .value = 0},
                               r);
    query_keys_read.push_back(key);
    return r;
}

int tx_region::tx_can_commit() {
    // TODO: Your code here
    if (!query_keys_write.size()) 
      return chdb_protocol::prepare_ok;

    for (int query_key : query_keys_write){
      int r;
      this->db->vserver->execute(query_key,
                                 chdb_protocol::CheckPrepareState,
                                 chdb_protocol::operation_var{.tx_id = tx_id, .key = 0, .value = 0},
                                 r);
      if (r == (int)false) 
        return chdb_protocol::prepare_not_ok;
    }
    return chdb_protocol::prepare_ok;
}

int tx_region::tx_begin() {
    // TODO: Your code here
    printf("tx[%d] begin\n", tx_id);
    return 0;
}

int tx_region::tx_commit() {
    // TODO: Your code here
    for (int query_key : query_keys_write) {
      int r;
      this->db->vserver->execute(query_key,
                                 chdb_protocol::Commit,
                                 chdb_protocol::operation_var{.tx_id = tx_id, .key = 0, .value = 0}, 
                                 r);
    }

    for (int query_key : query_keys_read) {
      int r;
      this->db->vserver->execute(query_key,
                                 chdb_protocol::Commit,
                                 chdb_protocol::operation_var{.tx_id = tx_id, .key = 0, .value = 0},
                                 r);
    }
    
    printf("tx[%d] commit\n", tx_id);
    return 0;
}

int tx_region::tx_abort() {
    // TODO: Your code here
    for (int query_key : query_keys_write) {
      int r;
      this->db->vserver->execute(query_key,
                                 chdb_protocol::Rollback,
                                 chdb_protocol::operation_var{.tx_id = tx_id, .key = 0, .value = 0}, 
                                 r);
    }

    printf("tx[%d] abort\n", tx_id);
    return 0;
}
