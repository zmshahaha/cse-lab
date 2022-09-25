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
#include "backward.hpp"
#define min(x, y) ((x) < (y) ? (x) : (y))
#define max(x, y) ((x) > (y) ? (x) : (y))
template<typename state_machine, typename command>
class raft {

static_assert(std::is_base_of<raft_state_machine, state_machine>(), "state_machine must inherit from raft_state_machine");
static_assert(std::is_base_of<raft_command, command>(), "command must inherit from raft_command");

friend class thread_pool;

#define RAFT_LOG(fmt, args...) \
    do { \
        printf("[%s:%d][node %d term %d][role %d] " fmt "\n", __FILE__, __LINE__, my_id, current_term,role, ##args); \
    } while(0);

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
    int commitIndex = 0;
    int lastApplied = 0;
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

    void send_heartbeat();
    void change_leader_commit();
    void init_leader();
    void change_current_term(int term);
    void print_log(); //just for debugging
private:
    bool is_stopped();
    int num_nodes() {return nodes;}

    // background workers    
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();

    // Your code here:


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

    // Register the rpcs.
    rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
    rpc_server->reg(raft_rpc_opcodes::op_append_entries, this, &raft::append_entries);
    rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this, &raft::install_snapshot);

    // Your code here: 
    // Do the initialization
    nodes = (int)clients.size();
    follower_election_timeout = my_id * 200 / (nodes-1) + 300;
    matchIndex.resize(nodes);
    nextIndex.resize(nodes);
    storage->read_data(voteFor,current_term,log);
    //{voteFor = -1;current_term = 0; log.resize(1);log[0].term = 0;log[0].index = 0;}
    //RAFT_LOG("log size:%d index:%d term:%d",(int)log.size(),log[0].index,log[0].term);
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

    //RAFT_LOG("start");
    this->background_election = new std::thread(&raft::run_background_election, this);
    this->background_ping = new std::thread(&raft::run_background_ping, this);
    this->background_commit = new std::thread(&raft::run_background_commit, this);
    this->background_apply = new std::thread(&raft::run_background_apply, this);
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term, int &index) {
    // Your code here:
    //RAFT_LOG("new command");
    std::unique_lock<std::mutex> guard(mtx);
    
    if(role != raft_role::leader){
        //RAFT_LOG("I received a command but I am not a leader");
        return false;
    }
    
    term = current_term;
    index = log.size();
    log.push_back({cmd,term,index});
    storage->persistent_log(log); // persist first, to avoid poweroff
    return true;
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot() {
    // Your code here:
    //RAFT_LOG("save snapshot");
    return true;
}



/******************************************************************

                         RPC Related

*******************************************************************/
template<typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args args, request_vote_reply& reply) {
    // Your code here:
    //RAFT_LOG("receive request vote from %d %d %d %d",args.candidateId,args.term,args.lastLogIndex,args.lastLogTerm);
    std::lock_guard<std::mutex> grd(mtx);
    //print_log();
    reply.vateGranted = false;
    reply.currentTerm = current_term;

    if(args.term < current_term){
        //RAFT_LOG("I refuse %d. It's term is %d.", args.candidateId, args.term);
        return 0;
    }
    
    if(args.term > current_term){
        role = raft_role::follower;
        change_current_term(args.term);
    }
    
    // a candidate can send vote req multiple time because of network failure
    if((voteFor == -1 || voteFor == args.candidateId) && role == follower)
    {
        int last_log_index = log.size() - 1;
        if(log[last_log_index].term > args.lastLogTerm || (log[last_log_index].term == args.lastLogTerm && log[last_log_index].index > args.lastLogIndex)){
            //RAFT_LOG("I refuse %d. It's term is %d.", args.candidateId, args.term);
            return 0;
        }
        //RAFT_LOG("I accept %d.I am a follower now.", args.candidateId);
        voteFor = args.candidateId;
        reply.vateGranted = true;
        
        // avoid unnecessary multiple candidate
        last_received_RPC_time = std::chrono::steady_clock::now();
        storage->persistent_metadata(voteFor,current_term);
    }
    
    return 0;
}


template<typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args& arg, const request_vote_reply& reply) {
    // Your code here:
    //RAFT_LOG("handle_request_vote_reply");
    //print_log();
    std::unique_lock<std::mutex> grd(mtx);
    if(reply.currentTerm > current_term){
        role = follower;
        change_current_term(reply.currentTerm);
        return;
    }

    // bug fixed: send heartbeat needs time, term can change and cause becoming leader in wrong term
    if(reply.vateGranted && role == candidate && current_term == arg.term){
        vote_count ++;
        if(vote_count >= nodes/2+1){
            init_leader();
            //RAFT_LOG("I am leader now.");
        }
    }
}


template<typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply& reply) {
    // Your code here:
    //RAFT_LOG("append entry from %d",arg.leaderId);
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
        
    if(arg.prevLogIndex > (int)log.size() - 1 || log[arg.prevLogIndex].term != arg.prevLogTerm)
        return 0;
    
    reply.success = true;

    // if leader and me is same
    if(arg.prevLogIndex != (int)log.size() - 1 || arg.entries.size() != 0)
        goto changecommit;    
    
    // don't worry about deleting the match entry because the last entry is the prevlog last time, which has been checked 
    log.resize(arg.prevLogIndex+arg.entries.size() + 1);
    std::copy(arg.entries.begin(),arg.entries.end(),log.begin() + arg.prevLogIndex + 1);
    storage->persistent_log(log);
    print_log();

    changecommit:
    commitIndex = max(commitIndex,min(arg.leaderCommit,arg.prevLogIndex+(int)arg.entries.size()));

    last_received_RPC_time = std::chrono::steady_clock::now();
    
    
    
    return 0;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int target, const append_entries_args<command>& arg, const append_entries_reply& reply) {
    // Your code here:
    //RAFT_LOG("handle_append_entries_reply");
    std::lock_guard<std::mutex> grd(mtx);
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
        nextIndex[target] = max(nextIndex[target] , arg.prevLogIndex+(int)arg.entries.size()+1);
        matchIndex[target] = nextIndex[target] - 1;
    }else if(nextIndex[target] > 150){
        nextIndex[target] -= 150;
    }else{
        nextIndex[target] = 1;
    }
}


template<typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args args, install_snapshot_reply& reply) {
    // Your code here:
    return 0;
}


template<typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int target, const install_snapshot_args& arg, const install_snapshot_reply& reply) {
    // Your code here:
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_request_vote(int target, request_vote_args arg) {
    //RAFT_LOG("send request vote");
    request_vote_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_request_vote, arg, reply) == 0) {
        handle_request_vote_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_append_entries(int target, append_entries_args<command> arg) {
    //RAFT_LOG("send append entry");
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

        if((role == candidate && timeval > candidate_election_timeout)|| 
            (role == follower && timeval > follower_election_timeout))
        {
            //RAFT_LOG("elect leader id %d term %d",my_id,current_term);
            std::unique_lock<std::mutex> grd(mtx);
            role = candidate;
            change_current_term(current_term+1);
            last_received_RPC_time=std::chrono::steady_clock::now();
            vote_count = 1;
            request_vote_args arg;
            arg.term = current_term;
            arg.candidateId = my_id;
            arg.lastLogIndex = log.size()-1;
            arg.lastLogTerm = log[arg.lastLogIndex].term;
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
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_commit() {
    // Send logs/snapshots to the follower.
    // Only work for the leader.

    // Hints: You should check the leader's last log index and the follower's next log index.        
    
    while (true) {
        if (is_stopped()) return;
        // Your code here:
        if (role == leader){
            std::unique_lock<std::mutex> grd(mtx);
            append_entries_args<command> arg;
            arg.leaderCommit = commitIndex;
            arg.leaderId = my_id;
            arg.term = current_term;
            //grd.unlock();
            for(int i=0;i<nodes;i++){
                if(i != my_id && (int)log.size() > matchIndex[i] + 1){
                    arg.prevLogIndex = nextIndex[i] - 1;
                    arg.prevLogTerm = log[arg.prevLogIndex].term;
                    //arg.entries.resize(1);
                    if((int)log.size() - 1 - arg.prevLogIndex > 150){
                        arg.entries.resize(150);
                        std::copy(log.begin() + arg.prevLogIndex + 1,log.begin() + arg.prevLogIndex + 151,arg.entries.begin());
                    }else{
                        arg.entries.resize((int)log.size() - 1 - arg.prevLogIndex);
                        std::copy(log.begin() + arg.prevLogIndex + 1,log.end(),arg.entries.begin());
                    }//std::cout<<"entrysize"<<arg.entries.size()<<std::endl;
                    //arg.entries.resize((int)log.size() - 1 - arg.prevLogIndex); std::cout<<"entrysize:"<<arg.entries.size()<<std::endl;
                    //std::copy(log.begin() + arg.prevLogIndex + 1,log.end(),arg.entries.begin());
                    //so when begin(nexindex=logsize),entrysize = 0 even log doesn't match
                    //std::cout<<"entrysize----:"<<arg.entries.size()<<std::endl;
                    thread_pool->addObjJob(this, &raft::send_append_entries,i,arg);
                }                
            }
            change_leader_commit();
            print_log();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }    
    
    return;
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
        
        if(commitIndex > lastApplied){std::unique_lock<std::mutex> grd(mtx);// may change in appendentry's copy
            for(; lastApplied < commitIndex ; lastApplied++){
RAFT_LOG("applying log %d",lastApplied+1);
                try{state->apply_log(log[lastApplied+1].cmd);}
                catch(const std::exception& e){std::cout<<e.what()<<std::endl; print_log();}
                RAFT_LOG("applying log %d fin",lastApplied+1);
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }    
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_ping() {
    // Send empty append_entries RPC to the followers.

    // Only work for the leader.
    
    while (true) {
        if (is_stopped()) return;
        // Your code here:
        if(role == leader){
            send_heartbeat();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(heartbeat_timeout)); // Change the timeout here!
    }
    return;
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
                if(log[matchIndex[i]].term == current_term){       //just can commit entry in my term
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
    std::fill(nextIndex.begin(),nextIndex.end(),log.size());
    std::fill(matchIndex.begin(),matchIndex.end(),0);
    send_heartbeat();
}


template<typename state_machine, typename command>
void raft<state_machine, command>::change_current_term(int term){
    current_term = term;
    voteFor = -1;
    storage->persistent_metadata(-1,current_term);
}

template<typename state_machine, typename command>
void raft<state_machine, command>::print_log(){
    char a[3000];
    int log_size = (int)log.size();
    sprintf(a,"size:%3d ",log_size);
    sprintf(a+9,"cmt:%3d apl:%3d ",commitIndex,lastApplied);
    
    for(int i = 0 ; i < log_size ; i++){
        sprintf(a+9+16+i*9,"%2d-%2d-%2d ",i,log[i].index,log[i].term);
    }
    RAFT_LOG("%s",a);
}


#endif // raft_h