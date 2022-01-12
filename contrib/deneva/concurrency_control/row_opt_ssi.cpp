/* Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: anduinzhu, axingguchen, xenitchen, hongyaozhao@tencent.com
 *
 */

#include "../system/txn.h"
#include "../storage/row.h"
#include "../system/manager.h"
#include "ssi.h"
#include "row_opt_ssi.h"
#include "../system/mem_alloc.h"
#include <jemalloc/jemalloc.h>
#include <atomic>

thread_local std::unordered_set<TxnManager*> Row_opt_ssi::visited{std::thread::hardware_concurrency()};
thread_local std::unordered_set<TxnManager*> Row_opt_ssi::visit_path{std::thread::hardware_concurrency() >= 32
                                                                          ? std::thread::hardware_concurrency() >> 4
                                                                          : std::thread::hardware_concurrency()};

thread_local RecycledTxnManagerSets Row_opt_ssi::empty_sets{};
thread_local atom::EpochGuard<Row_opt_ssi::NEMB, Row_opt_ssi::NEM>* Row_opt_ssi::neg_ = nullptr;



void Row_opt_ssi::init(row_t * row) {
    _row = row;
    blatch = false;
    latch = (pthread_mutex_t *) malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(latch, NULL);
    
    //for graphcc......
    alloc_ = new common::ChunkAllocator{};
    em_ = new atom::EpochManagerBase<common::ChunkAllocator>{alloc_};
    rw_history = new atom::AtomicSinglyLinkedList<uint64_t>(alloc_, em_);
    assert (rw_history != nullptr);
    lsn.store(0);
}


    
RC Row_opt_ssi::access(TxnManager * txn, TsType type, row_t * row) {
    RC rc = RCOK;
    // ts_t ts = txn->get_commit_timestamp();
    // ts_t start_ts = txn->get_start_timestamp();
    uint64_t starttime = get_sys_clock();
    txnid_t txnid = txn->get_txn_id();
    txnid_t my_txnid = txnid;
    std::unordered_set<uint64_t> oset;

    // if (g_central_man) {
    //     glob_manager.lock_row(_row);
    // } else {
    //     pthread_mutex_lock(latch);
    // }
    // INC_STATS(txn->get_thd_id(), trans_access_lock_wait_time, get_sys_clock() - starttime);

    //cur_node = txn;
    bool wrt_op = (type == R_REQ ? false : true);
    uint64_t prv;
    AccessInfo* accessInfo = (AccessInfo*)malloc(sizeof(AccessInfo));

    my_txnid = encode_txnid((uintptr_t)txn, wrt_op);
    prv = rw_history->push_front(my_txnid);
    accessInfo->which_rw_his = rw_history;

    if (prv > 0) {
      for (uint32_t i = 0; lsn.load() != prv; i++) {
        if (i >= 10000) std::this_thread::yield();
      }
    }

    assert(lsn.load() == prv);

    if (type == R_REQ) {
      //printf("in read OP (%ld, %ld)\n", lsn.load(), prv);
      bool cyclic = false;
      auto it = rw_history->begin();
      for (; it != rw_history->end(); ++it) {
        if (it.getId() < prv) {
          if (std::get<1>(find(*it)) &&
              !insert_and_check1(txn, std::get<0>(find(*it)), false)) {
            cyclic = true;
            break;
          }
        }
      }

      if (cyclic) {
        rw_history->erase(prv);
        lsn.store(prv + 1);
        abort(txn, oset);
        free(accessInfo);
        rc = Abort;
        goto end;
      }

      accessInfo->lsn = lsn.load();
      txn->txn->accessesInfo.add(accessInfo);
      lsn.fetch_add(1);

      // txn->cur_row = _row;
      assert(strstr(_row->get_table_name(), _row->get_table_name()));

      rc = RCOK;
      goto end;
    } else if (type == P_REQ) {
        //printf("in Write OP (%ld, %ld)\n", lsn.load(), prv);
begin_write:
        if (txn->abort_ || txn->cascading_abort_) {
          rw_history->erase(prv);
          lsn.store(prv + 1);
          abort(txn, oset);
          free(accessInfo);
          rc = Abort;
          goto end;
        }
        assert(lsn.load() == prv);
        bool aborted = false;
    
        // We need to delay w,w conflicts to be able to serialize the graph
        auto it_wait = rw_history->begin();
        auto it_end  = rw_history->end();
        assert(it_wait != it_end);
        bool cyclic = false, wait = false;

        while (!aborted && it_wait != it_end) {
          if (it_wait.getId() < prv && std::get<1>(find(*it_wait)) &&
              std::get<0>(find(*it_wait)) != uintptr_t(txn)) {
            if (!isCommited(std::get<0>(find(*it_wait)))) { //is commited?
              // ww-edge hence cascading abort necessary
              if (!insert_and_check1(txn, std::get<0>(find(*it_wait)), false)) {
                cyclic = true;
              }
              wait = true;
            }
          } 
          
          ++it_wait;
        }

        if (!aborted) {
          if (cyclic) {
cyclic_write:
             rw_history->erase(prv);
             lsn.store(prv + 1);
             abort(txn, oset);
             free(accessInfo);
             // std::cout << "abort(" << transaction << ") | rw" << std::endl;
             rc = Abort;
             goto end;
          }

          if (wait) {
            rw_history->erase(prv);
            lsn.store(prv + 1);
            prv = rw_history->push_front(my_txnid);
            if (prv > 0) {
              for (uint32_t i = 0; lsn.load() != prv; i++) {
                if (i >= 10000) std::this_thread::yield();
              }
            }
            goto begin_write;
          }

         auto it = rw_history->begin();
         auto end = rw_history->end();
         assert(rw_history->size() > 0);
         assert(it != rw_history->end());

         while (it != end) {
            if (it.getId() < prv) {
               // if it is read access this is a r-w edge, hence no cascading abort necessary
               if (!insert_and_check1(txn, std::get<0>(find(*it)), !std::get<1>(find(*it)))) {
                  cyclic = true;
               }
            }
            ++it;
         }

         if (cyclic) {
            goto cyclic_write;
         }
      }
      accessInfo->lsn = lsn.load();
      txn->txn->accessesInfo.add(accessInfo);
      lsn.fetch_add(1);
      goto end;
    }

end:
    uint64_t timespan = get_sys_clock() - starttime;
    txn->txn_stats.cc_time += timespan;
    txn->txn_stats.cc_time_short += timespan;

    // if (g_central_man) {
    //     glob_manager.release_row(_row);
    //  } else {
    //     pthread_mutex_unlock(latch);
    //  }

    //printf(">>>>>>rw historiy(%p) size is: %ld\n", rw_history, rw_history->size());
    //sleep(1);

    return rc;
}

bool Row_opt_ssi::find(TxnManager::NodeSet& nodes, TxnManager* transaction) const {
  auto it = nodes.begin();
  while (it != nodes.end()) {
    if (*it == transaction) {
      return true;
    }
    ++it;
  }
  return false;
}

// bool Row_opt_ssi::insert_and_check(uintptr_t from_node, bool readwrite) {
//   TxnManager* that_node = reinterpret_cast<TxnManager*>(from_node);
//   if (from_node == 0 || that_node == cur_node) {
//     return true;
//   }

//   while (true) {
//     if (!find(*cur_node->incoming_nodes_, accessEdge(that_node, readwrite))) {
//       if ((that_node->abort_ || that_node->cascading_abort_) && !readwrite) {
//         cur_node->cascading_abort_ = true;
//         cur_node->abort_through_ = from_node;
//         return false;
//       }

//       that_node->mut_.lock_shared();
//       if (that_node->cleaned_) {
//         that_node->mut_.unlock_shared();
//         return true;
//       }

//       if (that_node->checked_) {
//         that_node->mut_.unlock_shared();
//         continue;
//       }

//       cur_node->incoming_nodes_->insert(accessEdge(that_node, readwrite));
//       printf("txnid %ld's incoming txnid is %ld and count %ld>\n", 
//          cur_node->get_txn_id(), that_node->get_txn_id(), cur_node->incoming_nodes_->size());
//       that_node->outgoing_nodes_->insert(accessEdge(cur_node, readwrite));
//       that_node->mut_.unlock_shared();

//       bool cycle;
//       if (online_) {
//         cycle = !cycleCheckNaive();
//       } else {
//         cycle = !cycleCheckNaive();
//       }
//       return cycle;
//     }
//     return true;
//   }
// }

bool Row_opt_ssi::insert_and_check1(TxnManager* cur_node, uintptr_t from_node, bool readwrite) {
  TxnManager* that_node = reinterpret_cast<TxnManager*>(from_node);
  if (from_node == 0 || that_node == cur_node) {
    return true;
  }

  while (true) {
    if (!find(*cur_node->incoming_nodes_, accessEdge(that_node, readwrite))) {
      if ((that_node->abort_ || that_node->cascading_abort_) && !readwrite) {
        cur_node->cascading_abort_ = true;
        cur_node->abort_through_ = from_node;
        return false;
      }

      that_node->mut_.lock_shared();
      if (that_node->cleaned_.load() || that_node->commited_.load()) {
        that_node->mut_.unlock_shared();
        return true;
      }

      if (that_node->checked_.load()) {
        that_node->mut_.unlock_shared();
        continue;
      }

      cur_node->incoming_nodes_->insert(accessEdge(that_node, readwrite));
      that_node->outgoing_nodes_->insert(accessEdge(cur_node, readwrite));
      that_node->mut_.unlock_shared();

      
      bool cycle;
      if (online_) {
        cycle = !cycleCheckNaiveWrapper(cur_node);
      } else {
        cycle = !cycleCheckNaiveWrapper(cur_node);
      }
      return cycle;
    } 
    return true;
  }
}

bool Row_opt_ssi::cycleCheckNaiveWrapper(TxnManager* cur_node) {
  visited.clear();
  visit_path.clear();
  bool check = false;
  if (std::end(visited) == std::find(std::begin(visited), std::end(visited), cur_node)) {
    check |= cycleCheckNaive(cur_node);
  }
  return check;
}

bool Row_opt_ssi::cycleCheckNaive(TxnManager* cur) const {
 
  visited.insert(cur);
  visit_path.insert(cur);

  cur->mut_.lock_shared();
  if (!cur->cleaned_) {
    auto it = cur->incoming_nodes_->begin();
    while (it != cur->incoming_nodes_->end()) {
      auto node = std::get<0>(findEdge(*it));
      if (std::end(visit_path) != std::find(std::begin(visit_path), std::end(visit_path), node)) {
        cur->mut_.unlock_shared();
        return true;
      } else {
        if (cycleCheckNaive(node)) {
          cur->mut_.unlock_shared();
          return true;
        }
      }
      ++it;
    }
  }

  cur->mut_.unlock_shared();
  visit_path.erase(cur);
  return false;
}

bool Row_opt_ssi::needsAbort(uintptr_t cur) {
  auto node = reinterpret_cast<TxnManager*>(cur);
  return (node->cascading_abort_ || node->abort_);
}

bool Row_opt_ssi::isCommited(uintptr_t cur) {
  return reinterpret_cast<TxnManager*>(cur)->commited_.load();
}

void Row_opt_ssi::abort(TxnManager* cur_node, std::unordered_set<uint64_t>& oset) {
  
  cur_node->abort_.store(true);
  
// #ifdef 0
//   bool cascading = cur_node->cascading_abort_;
//   // logger.log(generateString());
//   if (cascading)
//     logger.log(common::LogInfo{reinterpret_cast<uintptr_t>(cur_node), 0, 0, 0, 'x'});
//   else
//     logger.log(common::LogInfo{reinterpret_cast<uintptr_t>(cur_node), 0, 0, 0, 'a'});
// #endif

  auto it_in = cur_node->incoming_nodes_->begin();
  while (it_in != cur_node->incoming_nodes_->end()) {
    if (!std::get<1>(findEdge(*it_in)))
      oset.emplace(reinterpret_cast<uintptr_t>(std::get<0>(findEdge(*it_in))));
    it_in++;
  }

  //cleanup(cur_node);

  oset.emplace(cur_node->abort_through_);
}

void Row_opt_ssi::cleanup(TxnManager* cur_node) {
  //cur_node->mut_.lock_shared();
  cur_node->cleaned_.store(true);
  //cur_node->mut_.unlock_shared();

  // barrier for inserts
  cur_node->mut_.lock();
  cur_node->mut_.unlock();

  auto it = cur_node->outgoing_nodes_->begin();
  while (it != cur_node->outgoing_nodes_->end()) {
    auto that_node = std::get<0>(findEdge(*it));
    if (cur_node->abort_.load() && !std::get<1>(findEdge(*it))) {
      that_node->cascading_abort_.store(true);
      that_node->abort_through_.store(reinterpret_cast<uintptr_t>(cur_node));
    } else {
      that_node->mut_.lock_shared();
      if (!that_node->cleaned_.load())
        that_node->incoming_nodes_->erase(accessEdge(cur_node, std::get<1>(findEdge(*it))));
      that_node->mut_.unlock_shared();
    }
    cur_node->outgoing_nodes_->erase(*it);
    ++it;
  }

  if (cur_node->abort_.load()) {
    auto it_out = cur_node->incoming_nodes_->begin();
    while (it_out != cur_node->incoming_nodes_->end()) {
      cur_node->incoming_nodes_->erase(*it_out);
      ++it_out;
    }
  }

  //atom::EpochGuard<EMB, EM> eg{em_};

  cur_node->mut_.lock();
  //empty_sets.rns->emplace_back(cur_node->outgoing_nodes_);
  //empty_sets.rns->emplace_back(cur_node->incoming_nodes_);

  if (cur_node->outgoing_nodes_->size() > 0 || cur_node->incoming_nodes_->size() > 0) {
    std::cout << "BROKEN" << std::endl;
  }

  assert(cur_node->outgoing_nodes_->size() == 0);
  assert(cur_node->incoming_nodes_->size() == 0);
  //cur_node->outgoing_nodes_ = nullptr;
  //cur_node->incoming_nodes_ = nullptr;

  // if (online_) {
  //   order_map_.erase(reinterpret_cast<uintptr_t>(cur_node));
  // }
  cur_node->mut_.unlock();

  // delete node;
  //eg.add(cur_node);
  // alloc_->deallocate(node, 1);
}

bool Row_opt_ssi::checkCommited(TxnManager* cur_node) {
  if (cur_node->abort_ || cur_node->cascading_abort_) {
    return false;
  }

  cur_node->mut_.lock_shared();
  cur_node->checked_ = true;
  cur_node->mut_.unlock_shared();

  // barrier for inserts
  cur_node->mut_.lock();
  cur_node->mut_.unlock();

  cur_node->mut_.lock_shared();
  if (cur_node->incoming_nodes_->size() != 0) {
    cur_node->checked_ = false;
    cur_node->mut_.unlock_shared();
    return false;
  }
  cur_node->mut_.unlock_shared();

  if (cur_node->abort_ || cur_node->cascading_abort_) {
    return false;
  }

  bool success = erase_graph_constraints(cur_node);

  if (success) {
    cleanup(cur_node);
  }
  return success;
}

bool Row_opt_ssi::erase_graph_constraints(TxnManager* cur_node) {
  if (cycleCheckNaiveWrapper(cur_node)) {
    cur_node->abort_ = true;
    return false;
  }

  cur_node->commited_ = true;

// #ifdef 0
//   // logger.log(generateString());
//   logger.log(common::LogInfo{reinterpret_cast<uintptr_t>(cur_node), 0, 0, 0, 'c'});
// #endif
  return true;
}

uintptr_t Row_opt_ssi::createNode(TxnManager* cur_node) {
  cur_node = alloc_->allocate<TxnManager>(1);

  TxnManager::NodeSet* sets[2];

  uint8_t i = 0;
  for (; i < 2 && !empty_sets.rns->empty();) {
    sets[i] = empty_sets.rns->back().release();
    empty_sets.rns->pop_back();
    i++;
  }
  for (; i < 2; i++) {
    created_sets_++;
    sets[i] = new TxnManager::NodeSet{std::thread::hardware_concurrency() >= 32 ? std::thread::hardware_concurrency() >> 4
                                                                          : std::thread::hardware_concurrency(),
                                alloc_, em_};
  }

  cur_node->incoming_nodes_ = sets[0];
  cur_node->outgoing_nodes_ = sets[1];

  if (online_) {
    order_map_.insert(reinterpret_cast<uintptr_t>(cur_node), std::numeric_limits<uint64_t>::max());
  }
  return reinterpret_cast<uintptr_t>(cur_node);
}


void Row_opt_ssi::setEMB(EMB* em) {
    em_ = em;
}

void Row_opt_ssi::setAlloc(Allocator* alloc) {
   alloc_ = alloc;
}


