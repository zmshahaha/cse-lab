#ifndef raft_h
#define raft_h

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <stdarg.h>

#include "rpc.h"
#include "raft_storage.h"
#include "raft_protocol.h"
#include "raft_state_machine.h"

#define MIN(x, y) ((x) < (y) ? (x) : (y))
#define MAX(x, y) ((x) > (y) ? (x) : (y))
#define MAX_ENTRY_SIZE 100

template<typename state_machine, typename command>
class raft {

static_assert(std::is_base_of<raft_state_machine, state_machine>(), "state_machine must inherit from raft_state_machine");
static_assert(std::is_base_of<raft_command, command>(), "command must inherit from raft_command");

friend class thread_pool;

#define RAFT_LOG(fmt, args...) \
    do { \
        printf("[%s:%d][node %d term %d][role %d] " fmt "\n", __FILE__, __LINE__, my_id, current_term,role, ##args); \
    } while(0);

#define ASSERT(condition, message) \
    do { \
        if (! (condition)) { \
            std::cerr << "Assertion `" #condition "` failed in " << __FILE__ \
                      << ":" << __LINE__ << " msg: " << message << std::endl; \
            std::terminate(); \
        } \
    } while (false)

public:
    raft(
        rpcs* rpc_server,
        std::vector<rpcc*> rpc_clients,
        int idx, 
        raft_storage<command>* storage,
        state_machine* state    
    );
    ~raft();

    // start the raft node.
    // Please make sure all of the rpc request handlers have been registered before this method.
    void start();

    // stop the raft node. 
    // Please make sure all of the background threads are joined in this method.
    // Notice: you should check whether is server should be stopped by calling is_stopped(). 
    //         Once it returns true, you should break all of your long-running loops in the background threads.
    void stop();

    // send a new command to the raft nodes.
    // This method returns true if this raft node is the leader that successfully appends the log.
    // If this node is not the leader, returns false. 
    bool new_command(command cmd, int &term, int &index);

    // returns whether this node is the leader, you should also set the current term;
    bool is_leader(int &term);

    // save a snapshot of the state machine and compact the log.
    bool save_snapshot();

private:
    std::mutex mtx;                     // A big lock to protect the whole data structure
    ThrPool* thread_pool;
    raft_storage<command>* storage;              // To persist the raft log
    state_machine* state;  // The state machine that applies the raft log, e.g. a kv store

    rpcs* rpc_server;               // RPC server to recieve and handle the RPC requests
    std::vector<rpcc*> rpc_clients; // RPC clients of all raft nodes including this node
    int my_id;                     // The index of this node in rpc_clients, start from 0

    std::atomic_bool stopped;

    enum raft_role {
        follower,
        candidate,
        leader
    };
    raft_role role;
    int current_term;

    std::thread* background_election;
    std::thread* background_ping;
    std::thread* background_commit;
    std::thread* background_apply;

    // Your code here:
    int voteFor;
    int heartbeat_timeout = 150;
    int follower_election_timeout;
    int candidate_election_timeout=1000;
    int vote_count = 0;
    std::chrono::time_point<std::chrono::steady_clock> last_received_RPC_time = std::chrono::steady_clock::now();
    std::vector<log_entry<command>> log;
    int commitIndex;
    int lastApplied;
    std::vector<int> nextIndex;
    std::vector<int> matchIndex;
    int nodes;

private:
    // RPC handlers
    int request_vote(request_vote_args arg, request_vote_reply& reply);

    int append_entries(append_entries_args<command> arg, append_entries_reply& reply);

    int install_snapshot(install_snapshot_args arg, install_snapshot_reply& reply);

    // RPC helpers
    void send_request_vote(int target, request_vote_args arg);
    void handle_request_vote_reply(int target, const request_vote_args& arg, const request_vote_reply& reply);

    void send_append_entries(int target, append_entries_args<command> arg);
    void handle_append_entries_reply(int target, const append_entries_args<command>& arg, const append_entries_reply& reply);

    void send_install_snapshot(int target, install_snapshot_args arg);
    void handle_install_snapshot_reply(int target, const install_snapshot_args& arg, const install_snapshot_reply& reply);

private:
    bool is_stopped();
    int num_nodes() {return nodes;}

    // background workers    
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();

    // Your code here:
    void send_heartbeat();
    void change_leader_commit();
    void init_leader();
    void change_current_term(int term);
    void print_log(std::string msg); //just for debugging
};

template<typename state_machine, typename command>
raft<state_machine, command>::raft(rpcs* server, std::vector<rpcc*> clients, int idx, raft_storage<command> *storage, state_machine *state) :
    storage(storage),
    state(state),   
    rpc_server(server),
    rpc_clients(clients),
    my_id(idx),
    stopped(false),
    role(follower),
    background_election(nullptr),
    background_ping(nullptr),
    background_commit(nullptr),
    background_apply(nullptr)
{
    thread_pool = new ThrPool(32);
    
    // Your code here: 
    // Do the initialization
    nodes = (int)clients.size();
    follower_election_timeout = my_id * 200 / ( nodes - 1 ) + 300;
    matchIndex.resize(nodes);
    nextIndex.resize(nodes);
    storage->read_metadata(voteFor,current_term);
    storage->read_log(log);
    
    // related to snapshot
    std::vector<char> data;
    storage->read_snapshot(log[0].index,log[0].term,data);
    if(data.size()!=0)
        state->apply_snapshot(data);
    lastApplied = commitIndex = log[0].index;
    
    // Register the rpcs.
    // other thread running rpcs can change data during init, so reg rpcs should be last work
    rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
    rpc_server->reg(raft_rpc_opcodes::op_append_entries, this, &raft::append_entries);
    rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this, &raft::install_snapshot);
}

template<typename state_machine, typename command>
raft<state_machine, command>::~raft() {
    if (background_ping) {
        delete background_ping;
    }
    if (background_election) {
        delete background_election;
    }
    if (background_commit) {
        delete background_commit;
    }
    if (background_apply) {
        delete background_apply;
    }
    delete thread_pool;    
}

/******************************************************************

                        Public Interfaces

*******************************************************************/

template<typename state_machine, typename command>
void raft<state_machine, command>::stop() {
    stopped.store(true);
    background_ping->join();
    background_election->join();
    background_commit->join();
    background_apply->join();
    thread_pool->destroy();
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::is_stopped() {
    return stopped.load();
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::is_leader(int &term) {
    term = current_term;
    return role == leader;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::start() {
    // Your code here:
    this->background_election = new std::thread(&raft::run_background_election, this);
    this->background_ping = new std::thread(&raft::run_background_ping, this);
    this->background_commit = new std::thread(&raft::run_background_commit, this);
    this->background_apply = new std::thread(&raft::run_background_apply, this);
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term, int &index) {
    // Your code here:
    std::unique_lock<std::mutex> guard(mtx);
    if(role != raft_role::leader)
        return false;
    
    term = current_term;
    index = log.size() + log[0].index;
    log.push_back({cmd,term,index});
    storage->persistent_log(log);
    return true;
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot() {
    // Your code here:
    std::unique_lock<std::mutex> grd(mtx);
    log[0].term = log[lastApplied-log[0].index].term;
    log.erase(log.begin() + 1 , log.begin() + 1 + lastApplied - log[0].index);
    log[0].index = lastApplied;
    std::vector<char> data = state -> snapshot();
    storage -> persistent_snapshot(log[0].index,log[0].term,data);
    storage -> persistent_log(log);
    return true;
}



/******************************************************************

                         RPC Related

*******************************************************************/
template<typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args args, request_vote_reply& reply) {
    // Your code here:
    std::unique_lock<std::mutex> grd(mtx);
    reply.vateGranted = false;
    reply.currentTerm = current_term;
    if(args.term < current_term)
        return 0;
    
    if(args.term > current_term){
        role = follower;
        change_current_term(args.term);
    }
    
    // main logic of reqvote
    // a candidate can send vote req multiple time because of network failure
    if((voteFor == -1 || voteFor == args.candidateId) && role == follower)
    {
        int last_log = log.size() - 1; // pos in log, not the index
        if(log[last_log].term > args.lastLogTerm || (log[last_log].term == args.lastLogTerm && log[last_log].index > args.lastLogIndex))
            return 0;
        voteFor = args.candidateId;
        reply.vateGranted = true;
        // avoid becoming unnecessary multiple candidate
        last_received_RPC_time = std::chrono::steady_clock::now();
        storage->persistent_metadata(voteFor,current_term);
    }
    
    return 0;
}


template<typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args& arg, const request_vote_reply& reply) {
    // Your code here:
    std::unique_lock<std::mutex> grd(mtx);
    if(reply.currentTerm > current_term){
        role = follower;
        change_current_term(reply.currentTerm);
        return;
    }

    // bug fixed: send heartbeat needs time, term can change and cause becoming leader in wrong term
    if(reply.vateGranted && role == candidate && current_term == arg.term){
        vote_count ++;
        if(vote_count >= nodes/2+1)
            init_leader();
    }
}


template<typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply& reply) {
    // Your code here:
    std::unique_lock<std::mutex> grd(mtx);
    reply.term = current_term;
    reply.success = false;
    if(arg.term < current_term)
        return 0;

    last_received_RPC_time = std::chrono::steady_clock::now();
    
    if(arg.term > current_term){
        change_current_term(arg.term);
        reply.term = current_term;
    }
    
    role = follower;

    // heartbeat
    if(arg.prevLogTerm == -1)
        goto changecommit;
        
    if(arg.prevLogIndex > log[0].index + (int)log.size() - 1 || log[arg.prevLogIndex - log[0].index].term != arg.prevLogTerm)
        return 0;
    
    reply.success = true;

    // if leader and me is same
    if(arg.prevLogIndex == (int)log.size() - 1 +log[0].index && arg.entries.size() == 0)
        goto changecommit;    
    
    // don't worry about deleting the match entry because the last entry is the prevlog last time, which has been checked 
    log.resize(arg.prevLogIndex+arg.entries.size() + 1 - log[0].index);
    std::copy(arg.entries.begin(),arg.entries.end(),log.begin() + arg.prevLogIndex + 1 - log[0].index);
    storage->persistent_log(log);

changecommit:
    // avoid new elected leader's cmt is too small or leader's matchidx[i](when ping, store in prevlogidx) doesn't up to date
    commitIndex = MAX(commitIndex,MIN(arg.leaderCommit,arg.prevLogIndex+(int)arg.entries.size()));
    last_received_RPC_time = std::chrono::steady_clock::now();
    return 0;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int target, const append_entries_args<command>& arg, const append_entries_reply& reply) {
    // Your code here:
    std::unique_lock<std::mutex> grd(mtx);
    if(reply.term > current_term){
        change_current_term(reply.term);
        role = follower;
        last_received_RPC_time = std::chrono::steady_clock::now();
        return;
    }

    // heartbeat reply
    if(arg.prevLogTerm == -1)
        return;

    if(reply.success == true){
        nextIndex[target] = MAX(nextIndex[target] , arg.prevLogIndex+(int)arg.entries.size()+1);
        matchIndex[target] = nextIndex[target] - 1;
        return;
    }
    
    // reply.success ==  false
    if(nextIndex[target] > log[0].index + MAX_ENTRY_SIZE){     
        nextIndex[target] -= MAX_ENTRY_SIZE;
    }else if(nextIndex[target] > log[0].index + 1){
        nextIndex[target] = log[0].index + 1;       // every nextidx > log[0].index must step through this stage,can't directly to log[0].index,otherwise delete some commit log
    }else{
        nextIndex[target] = log[0].index;           
    }
}


template<typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args args, install_snapshot_reply& reply) {
    // Your code here:
    std::unique_lock<std::mutex> grd(mtx);
    reply.term = current_term;
    if(args.term < current_term) 
        return 0;

    storage->persistent_snapshot(args.lastIncludedIndex,args.lastIncludedTerm,args.data);
    state->apply_snapshot(args.data);

    // clear the log
    log.resize(1);
    log[0].term = args.lastIncludedTerm;
    log[0].index = args.lastIncludedIndex;
    storage->persistent_log(log);

    commitIndex = lastApplied = log[0].index;
    return 0;
}


template<typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int target, const install_snapshot_args& arg, const install_snapshot_reply& reply) {
    // Your code here:
    std::unique_lock<std::mutex> grd(mtx);
    if(reply.term > current_term){
        change_current_term(reply.term);
        role = follower;
        return;
    }

    matchIndex[target] = arg.lastIncludedIndex;
    nextIndex[target] = arg.lastIncludedIndex + 1;
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_request_vote(int target, request_vote_args arg) {
    request_vote_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_request_vote, arg, reply) == 0) {
        handle_request_vote_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_append_entries(int target, append_entries_args<command> arg) {
    append_entries_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_append_entries, arg, reply) == 0) {
        handle_append_entries_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_install_snapshot(int target, install_snapshot_args arg) {
    install_snapshot_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_install_snapshot, arg, reply) == 0) {
        handle_install_snapshot_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

/******************************************************************

                        Background Workers

*******************************************************************/

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_election() {
    // Check the liveness of the leader.
    // Work for followers and candidates.

    // Hints: You should record the time you received the last RPC.
    //        And in this function, you can compare the current time with it.
    //        For example:
    //        if (current_time - last_received_RPC_time > timeout) start_election();
    //        Actually, the timeout should be different between the follower (e.g. 300-500ms) and the candidate (e.g. 1s).
    while (true) {
        if (is_stopped()) return;
        if (role == leader) continue;
        // Your code here:
        auto current_time = std::chrono::steady_clock::now();
        int timeval = std::chrono::duration_cast<std::chrono::milliseconds>(current_time-last_received_RPC_time).count();

        if((role == candidate && timeval > candidate_election_timeout) || (role == follower && timeval > follower_election_timeout))
        {
            std::unique_lock<std::mutex> grd(mtx);
            role = candidate;
            change_current_term(current_term+1);
            last_received_RPC_time=std::chrono::steady_clock::now();
            vote_count = 1;
            request_vote_args arg;
            arg.term = current_term;
            arg.candidateId = my_id;
            arg.lastLogIndex = log.size() + log[0].index -1;
            arg.lastLogTerm = log[arg.lastLogIndex - log[0].index].term;
            grd.unlock();
            for(int i=0;i<nodes;i++)
            {
                if(i!=my_id)
                {
                    thread_pool->addObjJob(this, &raft::send_request_vote,i,arg);
                }
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_commit() {
    // Send logs/snapshots to the follower.
    // Only work for the leader.

    // Hints: You should check the leader's last log index and the follower's next log index.        
    while (true) {
        if (is_stopped()) return;
        // Your code here:
        std::unique_lock<std::mutex> grd(mtx);
        if (role == leader){
            // I locked there before, but other thread can change my role after role == leader is true   
            append_entries_args<command> arg;
            arg.leaderCommit = commitIndex;
            arg.leaderId = my_id;
            arg.term = current_term;
            for(int i=0;i<nodes;i++){
                if(i == my_id || (int)log.size() +log[0].index <= matchIndex[i] + 1) continue;

                // if leader's log has been compressed to a snapshot, just send the snapshot
                if(nextIndex[i] < log[0].index + 1){
                    install_snapshot_args arg_snapshot;
                    arg_snapshot.term = current_term;
                    arg_snapshot.leaderId = my_id;
                    arg_snapshot.lastIncludedIndex = log[0].index;
                    arg_snapshot.lastIncludedTerm = log[0].term;
                    arg_snapshot.offset = 0;
                    arg_snapshot.data = state->snapshot();
                    arg_snapshot.done = 1;
                    thread_pool->addObjJob(this, &raft::send_install_snapshot,i,arg_snapshot);
                    continue;
                }
                
                // otherwise send log entry
                arg.prevLogIndex = nextIndex[i] - 1;
                arg.prevLogTerm = log[arg.prevLogIndex - log[0].index].term;
                if((int)log.size() - 1 - arg.prevLogIndex +log[0].index > MAX_ENTRY_SIZE){
                    arg.entries.resize(MAX_ENTRY_SIZE);
                    std::copy(log.begin() + arg.prevLogIndex + 1 - log[0].index,log.begin() + arg.prevLogIndex - log[0].index + 1 + MAX_ENTRY_SIZE,arg.entries.begin());
                }else{
                    arg.entries.resize((int)log.size() - 1 - arg.prevLogIndex + log[0].index);
                    std::copy(log.begin() + arg.prevLogIndex + 1 - log[0].index,log.end(),arg.entries.begin());
                }
                thread_pool->addObjJob(this, &raft::send_append_entries,i,arg);    
            }
            change_leader_commit();
        }
        grd.unlock(); // sleepfor can't be locked
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }    
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_apply() {
    // Apply committed logs the state machine
    // Work for all the nodes.

    // Hints: You should check the commit index and the apply index.
    //        Update the apply index and apply the log if commit_index > apply_index
    while (true) {
        if (is_stopped()) return;
        
        // Your code here:
        if(commitIndex > lastApplied){
            std::unique_lock<std::mutex> grd(mtx);
            for(; lastApplied < commitIndex ; lastApplied++){
                state->apply_log(log[lastApplied+1-log[0].index].cmd);
            }       
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }    
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_ping() {
    // Send empty append_entries RPC to the followers.

    // Only work for the leader.
    while (true) {
        if (is_stopped()) return;
        // Your code here:
        if(role == leader)
            send_heartbeat();
        std::this_thread::sleep_for(std::chrono::milliseconds(heartbeat_timeout)); // Change the timeout here!
    }
}


/******************************************************************

                        Other functions

*******************************************************************/

template<typename state_machine, typename command>
void raft<state_machine, command>::send_heartbeat() 
{
    append_entries_args<command> arg;
    arg.leaderId = my_id;
    arg.term = current_term;
    arg.leaderCommit = commitIndex;
    arg.prevLogTerm = -1; //mark heartbeat
    for(int i=0;i<nodes;i++){
        if(i!=my_id){
            arg.prevLogIndex = matchIndex[i] ; // remind follower to change commitindex
            thread_pool->addObjJob(this, &raft::send_append_entries,i,arg);
        }
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::change_leader_commit()
{
    // select matchindex's middle+1
    if(role == leader){
        int larger_match_commit_count;
        int smaller_match_commit_count;
        int range = (int)matchIndex.size();
        for(int i = 0 ; i < range; i++){
            larger_match_commit_count = 0;
            smaller_match_commit_count = 0;
            for(int j = 0 ; j < range ; j++){
                if(j == my_id || j == i) continue;
                if(matchIndex[i] > matchIndex[j]) smaller_match_commit_count++;
                if(matchIndex[i] < matchIndex[j]) larger_match_commit_count++;
            }
            if(smaller_match_commit_count <= range/2 && larger_match_commit_count < range/2){
                if(log[matchIndex[i] - log[0].index].term == current_term){       //just can commit entry in my term
                    commitIndex = matchIndex[i];
                }
                break;
            }
        }
    }
} 

template<typename state_machine, typename command>
void raft<state_machine, command>::init_leader(){
    role = leader;
    std::fill(nextIndex.begin(),nextIndex.end(),log.size()+ log[0].index);
    std::fill(matchIndex.begin(),matchIndex.end(),0);
    send_heartbeat();
    // send no-op 
    // int term,index;
    // new_command({},term,index);
}


template<typename state_machine, typename command>
void raft<state_machine, command>::change_current_term(int term){
    current_term = term;
    voteFor = -1;
    storage->persistent_metadata(-1,current_term);
}

template<typename state_machine, typename command>
void raft<state_machine, command>::print_log(std::string msg){
    char a[3000];
    msg = '(' + msg + ')';
    sprintf(a,"%s",msg.c_str());
    int log_size = (int)log.size();
    sprintf(a+msg.size(),"size:%3d ",log_size);
    sprintf(a+msg.size()+9,"cmt:%3d apl:%3d ",commitIndex,lastApplied);
    
    for(int i = 0 ; i < log_size ; i++){
        sprintf(a+msg.size()+9+16+i*15,"%3d-%3d-%3d-%2d ",i,log[i].index,log[i].term,log[i].cmd.size());
    }
    RAFT_LOG("%s",a);
}

#endif // raft_h