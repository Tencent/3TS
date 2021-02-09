#include <unistd.h>

#include "config.h"
#include "da.h"
#include "da_const.h"
#include "da_query.h"
#include "index_btree.h"
#include "index_hash.h"
#include "message.h"
#include "msg_queue.h"
#include "query.h"
#include "row.h"
#include "table.h"
#include "thread.h"
#include "transport.h"
#include "wl.h"

void DATxnManager::init(uint64_t thd_id, Workload *h_wl) {
    TxnManager::init(thd_id, h_wl);
    _wl = (DAWorkload *)h_wl;
    reset();
}

RC DATxnManager::run_txn_post_wait() {
    get_row_post_wait(row);
    return RCOK;
}

RC DATxnManager::acquire_locks() { return RCOK; }
RC DATxnManager::run_calvin_txn() { return RCOK; }
void DATxnManager::reset() {
  is_waiting_ = false;
  skip_queries_.clear();
  TxnManager::reset();
}

RC DATxnManager::process_query(const DAQuery* const da_query) {
    RC rc = RCOK;
    uint64_t trans_id = da_query->trans_id;
    uint64_t item_id = da_query->item_id;    // item_id from 0-2 represent X,Y,Z
    assert(trans_id < TRANS_CNT);
    assert(item_id < ITEM_CNT);
    //uint64_t seq_id = da_query->seq_id;
    uint64_t version = da_query->write_version;
    //uint64_t next_state = da_query->next_state;
    //uint64_t last_state = da_query->last_state;
    DATxnType txn_type = da_query->txn_type;

    //enum RC { RCOK = 0, Commit, Abort, WAIT, WAIT_REM, ERROR, FINISH, NONE };
    INDEX* index = _wl->i_datab;
    uint64_t value[ITEM_CNT];

    if (is_waiting_) {
        rc = WAIT;
    } else {
        switch (txn_type) {
            case DA_WRITE: {
                itemid_t* item = index_read(index, item_id, 0);
                assert(item != NULL);
                row_t* TempRow = ((row_t *)item->location);
                rc = get_row(TempRow, WR, row);
                if (rc == RCOK) {
                    row->set_value(VALUE, version);
                } else if (rc == Abort){
                    rc = start_abort();
                    already_abort_tab.insert(trans_id);
                }
                break;
            }
            case DA_READ: {
                itemid_t* item = index_read(index, item_id, 0);
                assert(item != NULL);
                row_t* TempRow = ((row_t *)item->location);
                rc = get_row(TempRow, RD, row);
                if (rc == RCOK) {
                    row->get_value(VALUE, value[0]);
                } else if (rc == Abort) {
                    rc = start_abort();
                    already_abort_tab.insert(trans_id);
                }
                break;
            }
            case DA_COMMIT: {
                rc = start_commit();
                break;
            }
            case DA_ABORT: {
                INC_STATS(get_thd_id(), positive_txn_abort_cnt, 1);
                rc = start_abort();
                break;
            }
            case DA_SCAN: {
                row_t *TempRow;
                for (int i = 0; i < ITEM_CNT && rc == RCOK; i++) {
                    itemid_t* item = index_read(index, i, 0);
                    assert(item != NULL);
                    TempRow = ((row_t *)item->location);
                    rc = get_row(TempRow, SCAN, row);
                    if (rc == RCOK) {
                        row->get_value(VALUE, value[i]);
                    } else if (rc == Abort) {
                        rc = start_abort();
                        already_abort_tab.insert(trans_id);
                    }
                }
                break;
            }
        }
    }

    // print operation
    switch (txn_type) {
        case DA_WRITE:
            DA_history_mem.push_back('W');
            break;
        case DA_READ:
            DA_history_mem.push_back('R');
            break;
        case DA_COMMIT:
            DA_history_mem.push_back('C');
            break;
        case DA_ABORT:
            DA_history_mem.push_back('A');
            break;
        case DA_SCAN:
            DA_history_mem.push_back('S');
            break;
    }
    DA_history_mem.push_back(static_cast<char>('0' + trans_id));//trans_id
    if (txn_type == DA_WRITE || txn_type == DA_READ) {
        DA_history_mem.push_back(static_cast<char>('a' + item_id));//item_id
    }
    DA_history_mem.push_back('=');

    // print result
    if (rc == Abort) {
        rc = start_abort();
        DA_history_mem += "Abort ";
        abort_history = true;
    } else if (rc == WAIT) {
        DA_history_mem += "Wait ";
        is_waiting_ = true;
        skip_queries_.emplace_back(*da_query);
    } else if (rc == RCOK) {
        if (txn_type == DA_WRITE) {
            DA_history_mem.push_back(static_cast<char>('0' + version));//item_id
        } else if (txn_type == DA_READ) {
            DA_history_mem.push_back(static_cast<char>('0' + value[0]));//item_id
        } else if (txn_type == DA_SCAN) {
            DA_history_mem.push_back('{');
            for (const auto v : value) {
                DA_history_mem.push_back(static_cast<char>('0' + v));//item_id
                DA_history_mem.push_back(',');
            }
            DA_history_mem.back() = '}';
        }
        DA_history_mem.push_back(' ');
    } else if (rc == Commit) {
        DA_history_mem += "Commit ";
    } else {
        DA_history_mem += "rc";
        DA_history_mem += std::to_string(rc);
        DA_history_mem.push_back(' ');
    }

    return rc;
}

RC DATxnManager::run_txn() {
#if MODE == SETUP_MODE
    return RCOK;
#endif
    //uint64_t starttime = get_sys_clock();
    if (IS_LOCAL(txn->txn_id)) {
          DEBUG("Running txn %ld\n", txn->txn_id);
#if DISTR_DEBUG
          query->print();
#endif
          query->partitions_touched.add_unique(GET_PART_ID(0, g_node_id));
    }
    if (!is_waiting_ && !skip_queries_.empty()) {
      // only happen in RTXN_CONT, and this->query has been stored in skip_queries_
      RC rc = RCOK;
      std::vector<DAQuery> skip_queries;
      std::swap(skip_queries_, skip_queries);
      for (const DAQuery& query : skip_queries) {
        DA_history_mem += "*";
        rc = process_query(&query);
        if (rc == Abort || rc == Commit) {
          return rc;
        }
      }
      return RCOK;
    } else {
      return process_query((DAQuery *)query);
    }
}
