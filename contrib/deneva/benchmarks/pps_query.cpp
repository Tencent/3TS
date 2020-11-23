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

#include "query.h"
#include "pps_query.h"
#include "pps.h"
#include "pps_helper.h"
#include "mem_alloc.h"
#include "wl.h"
#include "table.h"
#include "message.h"

BaseQuery * PPSQueryGenerator::create_query(Workload * h_wl,uint64_t home_partition_id) {
  double x = (double)(rand() % 100) / 100.0;
  if (x < g_perc_getparts) {
        return gen_requests_parts(home_partition_id);
  }
    if (x < g_perc_getparts + g_perc_getsuppliers) {
        return gen_requests_suppliers(home_partition_id);
  }
    if (x < g_perc_getparts + g_perc_getsuppliers + g_perc_getproducts) {
        return gen_requests_products(home_partition_id);
  }
    if (x < g_perc_getparts + g_perc_getsuppliers + g_perc_getproducts + g_perc_getpartbysupplier) {
        return gen_requests_partsbysupplier(home_partition_id);
  }
  if (x < g_perc_getparts + g_perc_getsuppliers + g_perc_getproducts + g_perc_getpartbysupplier +
              g_perc_getpartbyproduct) {
        return gen_requests_partsbyproduct(home_partition_id);
  }
  if (x < g_perc_getparts + g_perc_getsuppliers + g_perc_getproducts + g_perc_getpartbysupplier +
              g_perc_getpartbyproduct + g_perc_orderproduct) {
        return gen_requests_orderproduct(home_partition_id);
  }
  if (x < g_perc_getparts + g_perc_getsuppliers + g_perc_getproducts + g_perc_getpartbysupplier +
              g_perc_getpartbyproduct + g_perc_orderproduct + g_perc_updateproductpart) {
        return gen_requests_updateproductpart(home_partition_id);
  }
  if (x < g_perc_getparts + g_perc_getsuppliers + g_perc_getproducts + g_perc_getpartbysupplier +
              g_perc_getpartbyproduct + g_perc_orderproduct + g_perc_updateproductpart +
              g_perc_updatepart) {
        return gen_requests_updatepart(home_partition_id);
  }
  assert(false);
  return NULL;

}

void PPSQuery::init(uint64_t thd_id, Workload * h_wl) {
  BaseQuery::init();
  part_keys.init(MAX_PPS_PART_PER_PRODUCT);
}

void PPSQuery::init() {
  BaseQuery::init();
  part_keys.init(MAX_PPS_PART_PER_PRODUCT);
}

void PPSQuery::print() {
  std::cout << "part_key: " << part_key << "supplier_key: " << supplier_key
            << "product_key: " << product_key << std::endl;
}

std::set<uint64_t> PPSQuery::participants(Message * msg, Workload * wl) {
  std::set<uint64_t> participant_set;
  PPSClientQueryMessage* pps_msg = ((PPSClientQueryMessage*)msg);
  uint64_t id;

  switch(pps_msg->txn_type) {
    case PPS_GETPART:
      id = GET_NODE_ID(parts_to_partition(pps_msg->part_key));
      participant_set.insert(id);
      break;
    case PPS_GETPRODUCT:
      id = GET_NODE_ID(products_to_partition(pps_msg->product_key));
      participant_set.insert(id);
      break;
    case PPS_GETSUPPLIER:
      id = GET_NODE_ID(suppliers_to_partition(pps_msg->supplier_key));
      participant_set.insert(id);
      break;
    case PPS_GETPARTBYSUPPLIER:
      id = GET_NODE_ID(suppliers_to_partition(pps_msg->supplier_key));
      participant_set.insert(id);
      for (uint64_t key = 0; key < pps_msg->part_keys.size(); key++) {
          uint64_t tmp = pps_msg->part_keys[key];
          id = GET_NODE_ID(parts_to_partition(tmp));
          participant_set.insert(id);
      }
      break;
    case PPS_GETPARTBYPRODUCT:
      id = GET_NODE_ID(products_to_partition(pps_msg->product_key));
      participant_set.insert(id);
      for (uint64_t key = 0; key < pps_msg->part_keys.size(); key++) {
          uint64_t tmp = pps_msg->part_keys[key];
          id = GET_NODE_ID(parts_to_partition(tmp));
          participant_set.insert(id);
      }
      break;
    case PPS_ORDERPRODUCT:
      id = GET_NODE_ID(products_to_partition(pps_msg->product_key));
      participant_set.insert(id);
      for (uint64_t key = 0; key < pps_msg->part_keys.size(); key++) {
          uint64_t tmp = pps_msg->part_keys[key];
          id = GET_NODE_ID(parts_to_partition(tmp));
          participant_set.insert(id);
      }
      break;
    case PPS_UPDATEPRODUCTPART:
      id = GET_NODE_ID(products_to_partition(pps_msg->product_key));
      participant_set.insert(id);
      break;
    case PPS_UPDATEPART:
      id = GET_NODE_ID(parts_to_partition(pps_msg->part_key));
      participant_set.insert(id);
      break;
    default:
      assert(false);
  }

  return participant_set;
}

// Depreciated
uint64_t PPSQuery::participants(bool *& pps,Workload * wl) {
  int n = 0;
  for (uint64_t i = 0; i < g_node_cnt; i++) pps[i] = false;
  uint64_t id;

  switch(txn_type) {
    case PPS_GETPART:
      id = GET_NODE_ID(parts_to_partition(part_key));
      if(!pps[id]) {
        pps[id] = true;
        n++;
      }
      break;
    case PPS_GETPRODUCT:
      id = GET_NODE_ID(products_to_partition(product_key));
      if(!pps[id]) {
        pps[id] = true;
        n++;
      }
      break;
    case PPS_GETSUPPLIER:
      id = GET_NODE_ID(suppliers_to_partition(supplier_key));
      if(!pps[id]) {
        pps[id] = true;
        n++;
      }
      break;
    case PPS_GETPARTBYSUPPLIER:
      id = GET_NODE_ID(suppliers_to_partition(supplier_key));
      if(!pps[id]) {
        pps[id] = true;
        n++;
      }
      id = GET_NODE_ID(parts_to_partition(part_key));
      if(!pps[id]) {
        pps[id] = true;
        n++;
      }
      break;
    case PPS_GETPARTBYPRODUCT:
      id = GET_NODE_ID(products_to_partition(product_key));
      if(!pps[id]) {
        pps[id] = true;
        n++;
      }
      id = GET_NODE_ID(parts_to_partition(part_key));
      if(!pps[id]) {
        pps[id] = true;
        n++;
      }
      break;
    case PPS_ORDERPRODUCT:
      id = GET_NODE_ID(products_to_partition(product_key));
      if(!pps[id]) {
        pps[id] = true;
        n++;
      }
      id = GET_NODE_ID(parts_to_partition(part_key));
      if(!pps[id]) {
        pps[id] = true;
        n++;
      }
      break;

    default:
      assert(false);
  }

  return n;
}

bool PPSQuery::readonly() {
  if (txn_type == PPS_ORDERPRODUCT || txn_type == PPS_UPDATEPRODUCTPART ||
      txn_type == PPS_UPDATEPART) {
    return false;
  }
  return true;
}

BaseQuery * PPSQueryGenerator::gen_requests_parts(uint64_t home_partition) {
  PPSQuery * query = new PPSQuery;
    set<uint64_t> partitions_accessed;

    query->txn_type = PPS_GETPART;
  uint64_t part_key;
    // select a part
    if (FIRST_PART_LOCAL) {
    while (parts_to_partition(part_key = URand(1, g_max_part_key)) != home_partition) {
  }
  } else
        part_key = URand(1, g_max_part_key);

  query->part_key = part_key;
  partitions_accessed.insert(parts_to_partition(part_key));

  query->partitions.init(partitions_accessed.size());
  for(auto it = partitions_accessed.begin(); it != partitions_accessed.end(); ++it) {
    query->partitions.add(*it);
  }
  return query;
}

BaseQuery * PPSQueryGenerator::gen_requests_suppliers(uint64_t home_partition) {
  PPSQuery * query = new PPSQuery;
    set<uint64_t> partitions_accessed;

    query->txn_type = PPS_GETSUPPLIER;
  uint64_t supplier_key;
    // select a part
    if (FIRST_PART_LOCAL) {
    while (suppliers_to_partition(supplier_key = URand(1, g_max_supplier_key)) != home_partition) {
  }
  } else
        supplier_key = URand(1, g_max_supplier_key);

  query->supplier_key = supplier_key;
  partitions_accessed.insert(suppliers_to_partition(supplier_key));

  query->partitions.init(partitions_accessed.size());
  for(auto it = partitions_accessed.begin(); it != partitions_accessed.end(); ++it) {
    query->partitions.add(*it);
  }
  return query;
}


BaseQuery * PPSQueryGenerator::gen_requests_products(uint64_t home_partition) {
  PPSQuery * query = new PPSQuery;
    set<uint64_t> partitions_accessed;

    query->txn_type = PPS_GETPRODUCT;
  uint64_t product_key;
    // select a part
    if (FIRST_PART_LOCAL) {
    while (products_to_partition(product_key = URand(1, g_max_product_key)) != home_partition) {
  }
  } else
        product_key = URand(1, g_max_product_key);

  query->product_key = product_key;
  partitions_accessed.insert(products_to_partition(product_key));

  query->partitions.init(partitions_accessed.size());
  for(auto it = partitions_accessed.begin(); it != partitions_accessed.end(); ++it) {
    query->partitions.add(*it);
  }
  return query;
}

BaseQuery * PPSQueryGenerator::gen_requests_partsbysupplier(uint64_t home_partition) {
  PPSQuery * query = new PPSQuery;
    set<uint64_t> partitions_accessed;

    query->txn_type = PPS_GETPARTBYSUPPLIER;
  uint64_t supplier_key;
    // select a part
    if (FIRST_PART_LOCAL) {
    while (suppliers_to_partition(supplier_key = URand(1, g_max_supplier_key)) != home_partition) {
  }
  } else
        supplier_key = URand(1, g_max_supplier_key);

  query->supplier_key = supplier_key;
  partitions_accessed.insert(suppliers_to_partition(supplier_key));

  query->partitions.init(partitions_accessed.size());
  for(auto it = partitions_accessed.begin(); it != partitions_accessed.end(); ++it) {
    query->partitions.add(*it);
  }
  return query;
}

BaseQuery * PPSQueryGenerator::gen_requests_partsbyproduct(uint64_t home_partition) {
  PPSQuery * query = new PPSQuery;
    set<uint64_t> partitions_accessed;

    query->txn_type = PPS_GETPARTBYPRODUCT;
  uint64_t product_key;
    // select a part
    if (FIRST_PART_LOCAL) {
    while (products_to_partition(product_key = URand(1, g_max_product_key)) != home_partition) {
  }
  } else
        product_key = URand(1, g_max_product_key);

  query->product_key = product_key;
  partitions_accessed.insert(products_to_partition(product_key));

  query->partitions.init(partitions_accessed.size());
  for(auto it = partitions_accessed.begin(); it != partitions_accessed.end(); ++it) {
    query->partitions.add(*it);
  }
  return query;
}

BaseQuery * PPSQueryGenerator::gen_requests_orderproduct(uint64_t home_partition) {
  PPSQuery * query = new PPSQuery;
    set<uint64_t> partitions_accessed;

    query->txn_type = PPS_ORDERPRODUCT;
  uint64_t product_key;
    // select a part
    if (FIRST_PART_LOCAL) {
    while (products_to_partition(product_key = URand(1, g_max_product_key)) != home_partition) {
  }
  } else
        product_key = URand(1, g_max_product_key);

  query->product_key = product_key;
  partitions_accessed.insert(products_to_partition(product_key));

  query->partitions.init(partitions_accessed.size());
  for(auto it = partitions_accessed.begin(); it != partitions_accessed.end(); ++it) {
    query->partitions.add(*it);
  }
  return query;
}

BaseQuery * PPSQueryGenerator::gen_requests_updateproductpart(uint64_t home_partition) {
  PPSQuery * query = new PPSQuery;
    set<uint64_t> partitions_accessed;

    query->txn_type = PPS_UPDATEPRODUCTPART;
  uint64_t product_key;
    // select a part
    if (FIRST_PART_LOCAL) {
    while (products_to_partition(product_key = URand(1, g_max_product_key)) != home_partition) {
  }
  } else
        product_key = URand(1, g_max_product_key);

  query->product_key = product_key;
  partitions_accessed.insert(products_to_partition(product_key));
  query->part_key = URand(1, g_max_part_key);

  query->partitions.init(partitions_accessed.size());
  for(auto it = partitions_accessed.begin(); it != partitions_accessed.end(); ++it) {
    query->partitions.add(*it);
  }
  return query;
}


BaseQuery * PPSQueryGenerator::gen_requests_updatepart(uint64_t home_partition) {
  PPSQuery * query = new PPSQuery;
    set<uint64_t> partitions_accessed;

    query->txn_type = PPS_UPDATEPART;
  uint64_t part_key;
    // select a part
    if (FIRST_PART_LOCAL) {
    while (parts_to_partition(part_key = URand(1, g_max_part_key)) != home_partition) {
  }
  } else
        part_key = URand(1, g_max_part_key);

  query->part_key = part_key;
  partitions_accessed.insert(parts_to_partition(part_key));

  query->partitions.init(partitions_accessed.size());
  for(auto it = partitions_accessed.begin(); it != partitions_accessed.end(); ++it) {
    query->partitions.add(*it);
  }
  return query;
}


uint64_t PPSQuery::get_participants(Workload * wl) {
   uint64_t participant_cnt = 0;
   uint64_t active_cnt = 0;
  assert(participant_nodes.size()==0);
  assert(active_nodes.size()==0);
  for(uint64_t i = 0; i < g_node_cnt; i++) {
      participant_nodes.add(0);
      active_nodes.add(0);
  }
  assert(participant_nodes.size()==g_node_cnt);
  assert(active_nodes.size()==g_node_cnt);
  uint64_t id;

  if (txn_type == PPS_GETPART) {
      id = GET_NODE_ID(parts_to_partition(part_key));
      participant_nodes.set(id,1);
      participant_cnt++;
  }
  if (txn_type == PPS_GETPRODUCT) {
      id = GET_NODE_ID(products_to_partition(product_key));
      participant_nodes.set(id,1);
      participant_cnt++;
  }
  if (txn_type == PPS_GETSUPPLIER) {
      id = GET_NODE_ID(suppliers_to_partition(supplier_key));
      participant_nodes.set(id,1);
      participant_cnt++;
  }
  if (txn_type == PPS_GETPARTBYPRODUCT) {
      id = GET_NODE_ID(products_to_partition(product_key));
      participant_nodes.set(id,1);
      participant_cnt++;
      for (uint64_t key = 0; key < part_keys.size(); key++) {
          id = GET_NODE_ID(parts_to_partition(part_keys[key]));
          if (participant_nodes[id] == 0) {
              participant_nodes.set(id,1);
              participant_cnt++;
          }
      }
  }
  if (txn_type == PPS_GETPARTBYSUPPLIER) {
      id = GET_NODE_ID(suppliers_to_partition(supplier_key));
      participant_nodes.set(id,1);
      participant_cnt++;
      for (uint64_t key = 0; key < part_keys.size(); key++) {
          id = GET_NODE_ID(parts_to_partition(part_keys[key]));
          if (participant_nodes[id] == 0) {
              participant_nodes.set(id,1);
              participant_cnt++;
          }
      }
  }
  if (txn_type == PPS_ORDERPRODUCT) {
      id = GET_NODE_ID(products_to_partition(product_key));
      participant_nodes.set(id,1);
      participant_cnt++;
      for (uint64_t key = 0; key < part_keys.size(); key++) {
          id = GET_NODE_ID(parts_to_partition(part_keys[key]));
          if (participant_nodes[id] == 0) {
              participant_nodes.set(id,1);
              participant_cnt++;
          }
          if (active_nodes[id] == 0) {
              active_nodes.set(id,1);
              active_cnt++;
          }
      }
  }
  if (txn_type == PPS_UPDATEPRODUCTPART) {
      id = GET_NODE_ID(products_to_partition(product_key));
      participant_nodes.set(id,1);
      participant_cnt++;
      active_nodes.set(id,1);
      active_cnt++;
  }
  if (txn_type == PPS_UPDATEPART) {
      id = GET_NODE_ID(parts_to_partition(part_key));
      participant_nodes.set(id,1);
      participant_cnt++;
      active_nodes.set(id,1);
      active_cnt++;
  }

  return participant_cnt;
}

void PPSQuery::reset() {
  BaseQuery::clear();
  part_keys.clear();
}

void PPSQuery::release() {
  BaseQuery::release();
  part_keys.release();
  DEBUG_M("PPSQuery::release() free\n");
}

