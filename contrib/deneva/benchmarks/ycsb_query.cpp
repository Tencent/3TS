/*
     Tencent is pleased to support the open source community by making 3TS available.

     Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
     in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
     Tencent Modifications are Copyright (C) THL A29 Limited.

     Author: hongyaozhao@ruc.edu.cn

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

#include "query.h"
#include "ycsb_query.h"
#include "mem_alloc.h"
#include "wl.h"
#include "ycsb.h"
#include "table.h"
#include "helper.h"
#include "message.h"

uint64_t YCSBQueryGenerator::the_n = 0;
double YCSBQueryGenerator::denom = 0;

void YCSBQueryGenerator::init() {
    mrand = (myrand *) mem_allocator.alloc(sizeof(myrand));
    mrand->init(get_sys_clock());
    if (SKEW_METHOD == ZIPF) {
        zeta_2_theta = zeta(2, g_zipf_theta);
        uint64_t table_size = g_synth_table_size / g_part_cnt;
        the_n = table_size - 1;
        denom = zeta(the_n, g_zipf_theta);
    }
}

BaseQuery * YCSBQueryGenerator::create_query(Workload * h_wl, uint64_t home_partition_id) {
    BaseQuery * query;
    if (SKEW_METHOD == HOT) {
        query = gen_requests_hot(home_partition_id, h_wl);
    } else if (SKEW_METHOD == ZIPF){
        assert(the_n != 0);
        query = gen_requests_zipf(home_partition_id, h_wl);
    }

    return query;
}

void YCSBQuery::print() {

    for(uint64_t i = 0; i < requests.size(); i++) {
        printf("%d %ld, ",requests[i]->acctype,requests[i]->key);
    }
    printf("\n");
    /*
        printf("YCSBQuery: %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld\n"
                ,GET_NODE_ID(requests[0]->key)
                ,GET_NODE_ID(requests[1]->key)
                ,GET_NODE_ID(requests[2]->key)
                ,GET_NODE_ID(requests[3]->key)
                ,GET_NODE_ID(requests[4]->key)
                ,GET_NODE_ID(requests[5]->key)
                ,GET_NODE_ID(requests[6]->key)
                ,GET_NODE_ID(requests[7]->key)
                ,GET_NODE_ID(requests[8]->key)
                ,GET_NODE_ID(requests[9]->key)
                );
                */
}

void YCSBQuery::init() {
    requests.init(g_req_per_query);
    BaseQuery::init();
}

void YCSBQuery::copy_request_to_msg(YCSBQuery * ycsb_query, YCSBQueryMessage * msg, uint64_t id) {
/*
    DEBUG_M("YCSBQuery::copy_request_to_msg ycsb_request alloc\n");
    ycsb_request * req = (ycsb_request*) mem_allocator.alloc(sizeof(ycsb_request));
    req->copy(ycsb_query->requests[id]);
    msg->requests.add(req);
*/
    msg->requests.add(ycsb_query->requests[id]);
}

void YCSBQuery::release_requests() {
    // A bit of a hack to ensure that original requests in client query queue aren't freed
    if (SERVER_GENERATE_QUERIES && requests.size() == g_req_per_query) return;
    for(uint64_t i = 0; i < requests.size(); i++) {
        DEBUG_M("YCSBQuery::release() ycsb_request free\n");
        mem_allocator.free(requests[i],sizeof(ycsb_request));
    }

}

void YCSBQuery::reset() {
    BaseQuery::clear();
#if CC_ALG != CALVIN
    release_requests();
#endif
    requests.clear();
}

void YCSBQuery::release() {
    BaseQuery::release();
    DEBUG_M("YCSBQuery::release() free\n");
#if CC_ALG != CALVIN
    release_requests();
#endif
    requests.release();
}

uint64_t YCSBQuery::get_participants(Workload * wl) {

    uint64_t participant_cnt = 0;
    assert(participant_nodes.size()==0);
    assert(active_nodes.size()==0);
    for(uint64_t i = 0; i < g_node_cnt; i++) {
            participant_nodes.add(0);
            active_nodes.add(0);
    }
    assert(participant_nodes.size()==g_node_cnt);
    assert(active_nodes.size()==g_node_cnt);
    for(uint64_t i = 0; i < requests.size(); i++) {
        uint64_t req_nid = GET_NODE_ID(((YCSBWorkload*)wl)->key_to_part(requests[i]->key));
        if(requests[i]->acctype == RD) {
            if (participant_nodes[req_nid] == 0) ++participant_cnt;
            participant_nodes.set(req_nid,1);
        }
        if (requests[i]->acctype == WR) active_nodes.set(req_nid, 1);
    }
    return participant_cnt;
}

uint64_t YCSBQuery::participants(bool *& pps,Workload * wl) {
    int n = 0;
    for (uint64_t i = 0; i < g_node_cnt; i++) pps[i] = false;

    for(uint64_t i = 0; i < requests.size(); i++) {
        uint64_t req_nid = GET_NODE_ID(((YCSBWorkload*)wl)->key_to_part(requests[i]->key));
        if (!pps[req_nid]) n++;
        pps[req_nid] = true;
    }
    return n;
}

std::set<uint64_t> YCSBQuery::participants(Message * msg, Workload * wl) {
    std::set<uint64_t> participant_set;
    YCSBClientQueryMessage* ycsb_msg = ((YCSBClientQueryMessage*)msg);
    for(uint64_t i = 0; i < ycsb_msg->requests.size(); i++) {
        uint64_t req_nid = GET_NODE_ID(((YCSBWorkload*)wl)->key_to_part(ycsb_msg->requests[i]->key));
        participant_set.insert(req_nid);
    }
    return participant_set;
}

bool YCSBQuery::readonly() {
    for(uint64_t i = 0; i < requests.size(); i++) {
        if(requests[i]->acctype == WR) {
            return false;
        }
    }
    return true;
}

// The following algorithm comes from the paper:
// Quickly generating billion-record synthetic databases
// However, it seems there is a small bug.
// The original paper says zeta(theta, 2.0). But I guess it should be
// zeta(2.0, theta).
double YCSBQueryGenerator::zeta(uint64_t n, double theta) {
    double sum = 0;
    for (uint64_t i = 1; i <= n; i++) sum += pow(1.0 / i, theta);
    return sum;
}

uint64_t YCSBQueryGenerator::zipf(uint64_t n, double theta) {
    assert(this->the_n == n);
    assert(theta == g_zipf_theta);
    double alpha = 1 / (1 - theta);
    double zetan = denom;
    double eta = (1 - pow(2.0 / n, 1 - theta)) / (1 - zeta_2_theta / zetan);
//    double eta = (1 - pow(2.0 / n, 1 - theta)) /
//        (1 - zeta_2_theta / zetan);
    double u = (double)(mrand->next() % 10000000) / 10000000;
    double uz = u * zetan;
    if (uz < 1) return 1;
    if (uz < 1 + pow(0.5, theta)) return 2;
    return 1 + (uint64_t)(n * pow(eta*u -eta + 1, alpha));
}


BaseQuery * YCSBQueryGenerator::gen_requests_hot(uint64_t home_partition_id, Workload * h_wl) {
    YCSBQuery * query = (YCSBQuery*) mem_allocator.alloc(sizeof(YCSBQuery));
    query->requests.init(g_req_per_query);

    uint64_t access_cnt = 0;
    set<uint64_t> all_keys;
    set<uint64_t> partitions_accessed;
    double r_mpt = (double)(mrand->next() % 10000) / 10000;
    uint64_t part_limit;
    if(r_mpt < g_mpr)
        part_limit = g_part_per_txn;
    else
        part_limit = 1;
    uint64_t hot_key_max = (uint64_t)g_data_perc;
    double r_twr = (double)(mrand->next() % 10000) / 10000;

    int rid = 0;
    for (UInt32 i = 0; i < g_req_per_query; i ++) {
        double r = (double)(mrand->next() % 10000) / 10000;
        double hot =  (double)(mrand->next() % 10000) / 10000;
        uint64_t partition_id;
        ycsb_request * req = (ycsb_request*) mem_allocator.alloc(sizeof(ycsb_request));
        if (r_twr < g_txn_read_perc || r < g_tup_read_perc)
            req->acctype = RD;
        else
            req->acctype = WR;

        uint64_t row_id = 0;
        if ( FIRST_PART_LOCAL && rid == 0) {
            if(hot < g_access_perc) {
                row_id =
                        (uint64_t)(mrand->next() % (hot_key_max / g_part_cnt)) * g_part_cnt + home_partition_id;
            } else {
                uint64_t nrand = (uint64_t)mrand->next();
                row_id = ((nrand % (g_synth_table_size / g_part_cnt - (hot_key_max / g_part_cnt))) +
                                    hot_key_max / g_part_cnt) *
                                         g_part_cnt +
                                 home_partition_id;
            }

            partition_id = row_id % g_part_cnt;
            assert(row_id % g_part_cnt == home_partition_id);
        } else {
            while(1) {
                if(hot < g_access_perc) {
                    row_id = (uint64_t)(mrand->next() % hot_key_max);
                } else {
                    row_id = ((uint64_t)(mrand->next() % (g_synth_table_size - hot_key_max))) + hot_key_max;
                }
                partition_id = row_id % g_part_cnt;

                if (g_strict_ppt && partitions_accessed.size() < part_limit &&
                        (partitions_accessed.size() + (g_req_per_query - rid) >= part_limit) &&
                        partitions_accessed.count(partition_id) > 0)
                    continue;
                if (partitions_accessed.count(partition_id) > 0) break;
                if (partitions_accessed.size() == part_limit) continue;
                break;
            }
        }
        partitions_accessed.insert(partition_id);
        assert(row_id < g_synth_table_size);
        uint64_t primary_key = row_id;
        //uint64_t part_id = row_id % g_part_cnt;
        req->key = primary_key;
        req->value = mrand->next() % (1<<8);
        // Make sure a single row is not accessed twice
        if (all_keys.find(req->key) == all_keys.end()) {
            all_keys.insert(req->key);
            access_cnt ++;
        } else {
            // Need to have the full g_req_per_query amount
            i--;
            continue;
        }
        rid ++;

        //query->requests.push_back(*req);
        query->requests.add(req);
    }
    assert(query->requests.size() == g_req_per_query);
    // Sort the requests in key order.
    if (g_key_order) {
        for(uint64_t i = 0; i < query->requests.size(); i++) {
            for(uint64_t j = query->requests.size() - 1; j > i ; j--) {
                if(query->requests[j]->key < query->requests[j-1]->key) {
                    query->requests.swap(j,j-1);
                }
            }
        }
        // std::sort(query->requests.begin(),query->requests.end(),[](ycsb_request lhs, ycsb_request
        // rhs) { return lhs.key < rhs.key;});
    }
    query->partitions.init(partitions_accessed.size());
    for(auto it = partitions_accessed.begin(); it != partitions_accessed.end(); ++it) {
        query->partitions.add(*it);
    }

    return query;

}

BaseQuery * YCSBQueryGenerator::gen_requests_zipf(uint64_t home_partition_id, Workload * h_wl) {
    YCSBQuery * query = (YCSBQuery*) mem_allocator.alloc(sizeof(YCSBQuery));
    new(query) YCSBQuery();
    query->requests.init(g_req_per_query);

    uint64_t access_cnt = 0;
    set<uint64_t> all_keys;
    set<uint64_t> partitions_accessed;
    uint64_t table_size = g_synth_table_size / g_part_cnt;

    double r_twr = (double)(mrand->next() % 10000) / 10000;

    int rid = 0;
    for (UInt32 i = 0; i < g_req_per_query; i ++) {
        double r = (double)(mrand->next() % 10000) / 10000;
        uint64_t partition_id;
#ifdef LESS_DIS
    if ( rid < 5) {
        partition_id = home_partition_id;;
    } else {
        partition_id = (home_partition_id + 1) % g_part_cnt;
    }
#else
    #ifdef NO_REMOTE
        partition_id = home_partition_id;
    #else
        if ( FIRST_PART_LOCAL && rid == 0) {
            partition_id = home_partition_id;
        } else {
            partition_id = mrand->next() % g_part_cnt;
            if(g_strict_ppt && g_part_per_txn <= g_part_cnt) {
                while ((partitions_accessed.size() < g_part_per_txn &&
                                partitions_accessed.count(partition_id) > 0) ||
                             (partitions_accessed.size() == g_part_per_txn &&
                                partitions_accessed.count(partition_id) == 0)) {
                    partition_id = mrand->next() % g_part_cnt;
                }
            }
        }
    #endif
#endif
        ycsb_request * req = (ycsb_request*) mem_allocator.alloc(sizeof(ycsb_request));
        if (r_twr < g_txn_read_perc || r < g_tup_read_perc)
            req->acctype = RD;
        else
            req->acctype = WR;
        uint64_t row_id = zipf(table_size - 1, g_zipf_theta);
        assert(row_id < table_size);
        uint64_t primary_key = row_id * g_part_cnt + partition_id;
        assert(primary_key < g_synth_table_size);

        req->key = primary_key;
        req->value = mrand->next() % (1<<8);
        // Make sure a single row is not accessed twice
        if (all_keys.find(req->key) == all_keys.end()) {
            all_keys.insert(req->key);
            access_cnt ++;
        } else {
            // Need to have the full g_req_per_query amount
            i--;
            continue;
        }
        partitions_accessed.insert(partition_id);
        rid ++;

        query->requests.add(req);
    }
    assert(query->requests.size() == g_req_per_query);
    // Sort the requests in key order.
    if (g_key_order) {
        for(uint64_t i = 0; i < query->requests.size(); i++) {
            for(uint64_t j = query->requests.size() - 1; j > i ; j--) {
                if(query->requests[j]->key < query->requests[j-1]->key) {
                    query->requests.swap(j,j-1);
                }
            }
        }
        // std::sort(query->requests.begin(),query->requests.end(),[](ycsb_request lhs, ycsb_request
        // rhs) { return lhs.key < rhs.key;});
    }
    query->partitions.init(partitions_accessed.size());
    for(auto it = partitions_accessed.begin(); it != partitions_accessed.end(); ++it) {
        query->partitions.add(*it);
    }

    //query->print();
    return query;

}
