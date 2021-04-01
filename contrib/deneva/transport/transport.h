/*
   Copyright 2016 Massachusetts Institute of Technology

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#ifndef _TRANSPORT_H_
#define _TRANSPORT_H_
#include "global.h"
#include "nn.hpp"
#include <nanomsg/bus.h>
#include <nanomsg/pair.h>
#include "query.h"

class Workload;
class Message;

/*
     Message format:
Header: 4 Byte receiver ID
                4 Byte sender ID
                4 Byte # of bytes in msg
Data:    MSG_SIZE - HDR_SIZE bytes
     */

#define GET_RCV_NODE_ID(b)  ((uint32_t*)b)[0]

class Socket {
public:
    Socket () : sock(AF_SP,NN_PAIR) {}
    ~Socket () {}
    char _pad1[CL_SIZE];
    nn::socket sock;
    char _pad[CL_SIZE - sizeof(nn::socket)];
};

class Transport {
public:
    void read_ifconfig(const char * ifaddr_file);
    void init();
    void shutdown();
    uint64_t get_socket_count();
    string get_path();
    Socket * get_socket();
    uint64_t get_port_id(uint64_t src_node_id, uint64_t dest_node_id);
    uint64_t get_port_id(uint64_t src_node_id, uint64_t dest_node_id, uint64_t send_thread_id);
    Socket * bind(uint64_t port_id);
    Socket * connect(uint64_t dest_id,uint64_t port_id);
    void send_msg(uint64_t send_thread_id, uint64_t dest_node_id, void * sbuf,int size);
    std::vector<Message*> * recv_msg(uint64_t thd_id);
    void simple_send_msg(int size);
    uint64_t simple_recv_msg();

private:
    uint64_t rr;
    std::map<std::pair<uint64_t, uint64_t>, Socket*> send_sockets;  // dest_node_id,send_thread_id :
                                                                  // socket
    std::vector<Socket*> recv_sockets;

    uint64_t _node_cnt;
    uint64_t _sock_cnt;
    uint64_t _s_cnt;
    char ** ifaddr;
    int * endpoint_id;

};

#endif
