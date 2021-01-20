#include "dli.h"

#include "../system/helper.h"
#include "../system/manager.h"
#include "../system/global.h"
#include "dta.h"
#include "row_dli_based.h"
#include "txn.h"
#if CC_ALG == DLI_BASE || CC_ALG == DLI_OCC || CC_ALG == DLI_MVCC_OCC || CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3 || \
    CC_ALG == DLI_MVCC
void dli_merge_set(Dli::RWSet& r_tc,
                   Dli::RWSet& r_tl) {
  r_tc.insert(r_tl.begin(), r_tl.end());
}

void dli_compare_set(const Dli::RWSet& r_tc, uint32_t& newer_tc,
                     const Dli::RWSet& r_tl, uint32_t& newer_tl, bool rw,
                     const uint32_t offset) {
  for (const auto& i : r_tl) {
    if (r_tc.count(i.first)) {
      newer_tl |= (i.second > r_tc.at(i.first)) << offset;
      if (!rw)
        newer_tc |= (i.second < r_tc.at(i.first)) << offset;
      else
        newer_tc |= (i.second <= r_tc.at(i.first)) << offset;
    }
  }
}

#define IDENTIFY_ANOMALY(tc_off, tl_off, ano_name)                      \
  do {                                                                  \
    if ((newer_tc & (1 << (tc_off))) && (newer_tl & (1 << (tl_off)))) { \
      INC_STATS(txn->get_thd_id(), ano_name, 1);                        \
      return true;                                                      \
    }                                                                   \
  } while (0)

bool dli_has_conflict(TxnManager* txn, const uint32_t newer_tc, const uint32_t newer_tl) {
  if (!newer_tc || !newer_tl) return false;
  IDENTIFY_ANOMALY(3, 3, ano_4_trans_read_skew);
  IDENTIFY_ANOMALY(3, 1, ano_3_trans_read_skew_1);
  IDENTIFY_ANOMALY(2, 3, ano_3_trans_write_skew_1);
  IDENTIFY_ANOMALY(2, 1, ano_2_trans_write_skew_1);
  IDENTIFY_ANOMALY(1, 3, ano_3_trans_read_skew_2);
  IDENTIFY_ANOMALY(1, 1, ano_2_trans_read_skew);
  IDENTIFY_ANOMALY(0, 3, ano_3_trans_write_skew_2);
  IDENTIFY_ANOMALY(0, 1, ano_2_trans_write_skew_2);
  INC_STATS(txn->get_thd_id(), ano_unknown, 1);
  return true;
}

#undef IDENTIFY_ANOMALY
#if CC_ALG == DLI_BASE || CC_ALG == DLI_OCC || CC_ALG == DLI_MVCC_OCC || CC_ALG == DLI_MVCC
static bool check_conflict(TxnManager* txn, Dli::RWSet& r_tc,
                           Dli::RWSet& w_tc,
                           Dli::RWSet& r_tl,
                           Dli::RWSet& w_tl) {
  //(txn, rset, wset, r_tl, w_tl);
  //r_tc:rset w_tc:wset
  //
  uint32_t newer_tl = 0, newer_tc = 0;
  dli_compare_set(r_tc, newer_tc, r_tl, newer_tl, false, 3);
  if (dli_has_conflict(txn, newer_tc, newer_tl)) return true;
  dli_compare_set(w_tc, newer_tc, w_tl, newer_tl, false, 2);
  if (dli_has_conflict(txn, newer_tc, newer_tl)) return true;
  dli_compare_set(r_tc, newer_tc, w_tl, newer_tl, true, 1);
  if (dli_has_conflict(txn, newer_tc, newer_tl)) return true;
  dli_compare_set(r_tl, newer_tl, w_tc, newer_tc, true, 0);
  if (dli_has_conflict(txn, newer_tc, newer_tl)) return true;

  dli_merge_set(r_tc, r_tl);
  dli_merge_set(w_tc, w_tl);

  return false;
}

#endif

#if CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3
static bool check_conflict(TxnManager* txn, Dli::RWSet& r_tc,
                           Dli::RWSet& w_tc,
                           Dli::RWSet& r_tl,
                           Dli::RWSet& w_tl, uint64_t& lower,
                           uint64_t& upper, uint64_t lower_tl, uint64_t upper_tl) {
  uint32_t newer_tl = 0, newer_tc = 0;

  dli_compare_set(r_tc, newer_tc, w_tl, newer_tl, true, 1);
  if (dli_has_conflict(txn, newer_tc, newer_tl)) return true;
  dli_compare_set(r_tl, newer_tl, w_tc, newer_tc, true, 0);
  if (dli_has_conflict(txn, newer_tc, newer_tl)) return true;
  if (newer_tc)
    lower = std::max(lower, upper_tl);
  else if (newer_tl)
    upper = std::min(lower_tl, upper);

  if (lower >= upper) return true;
  dli_compare_set(r_tc, newer_tc, r_tl, newer_tl, false, 3);
  if (dli_has_conflict(txn, newer_tc, newer_tl)) return true;
#if CC_ALG != DLI_DTA3
  dli_compare_set(w_tc, newer_tc, w_tl, newer_tl, false, 2);
  if (dli_has_conflict(txn, newer_tc, newer_tl)) return true;
#endif

#if CC_ALG == DLI_DTA
  dli_merge_set(r_tc, r_tl);
  dli_merge_set(w_tc, w_tl);
#endif
  return false;
}
#endif
#endif
static RC validate_main(TxnManager* txn, Dli* dli, const bool final_validate) {
  RC rc = RCOK;
#if CC_ALG == DLI_MVCC_OCC || CC_ALG == DLI_BASE || CC_ALG == DLI_OCC || CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3 || \
    CC_ALG == DLI_MVCC
  uint64_t start_time = get_sys_clock();
  uint64_t expect = 0;
  Dli::RWSet rset, wset;
  std::unordered_set<ts_t> cur_trans;
  Dli::get_rw_set(txn, rset, wset);
  Dli::RWSet rset_bak = rset, wset_bak = wset;
  ts_t ts = txn->get_start_timestamp();

#if CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3
  uint64_t tid = txn->get_thd_id();
  txnid_t txnid = txn->get_txn_id();
  uint64_t lower = dta_time_table.get_lower(tid, txnid);
  uint64_t upper = dta_time_table.get_upper(tid, txnid);
  if (lower >= upper) rc = Abort;
#endif

  uint64_t timespan = get_sys_clock() - start_time;
  txn->txn_stats.cc_time += timespan;
  txn->txn_stats.cc_time_short += timespan;
  start_time += timespan;

  timespan = get_sys_clock() - start_time;
  txn->txn_stats.cc_block_time += timespan;
  txn->txn_stats.cc_block_time_short += timespan;
  start_time += timespan;

  if (rc == RCOK) {
    for (auto& i : wset) {
      if (i.first->manager->w_trans != ts) {
        if (!i.first->manager->w_trans.compare_exchange_weak(expect, ts)) {
          rc = Abort;
          break;
        }
      }
    }
  }

  if (rc == RCOK && !wset.empty()) {
    for (auto& i : wset) {
      i.second = UINT64_MAX;
#if CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3
      row_t* cur_wrow = i.first;
      if (lower <= cur_wrow->manager->timestamp_last_write) {
        lower = cur_wrow->manager->timestamp_last_write + 1;
      }
#endif
    }
    bool res = false;
    for (TSNode<Dli::ValidatedTxn>* tl = dli->validated_txns_.load();
        tl != nullptr; //&& tl->start_ts_ > ts;
        tl = tl->next_) {//cur_trans is active tran table
      if (tl->is_abort_.load()) continue;
#if CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3
      res = check_conflict(txn, rset, wset, tl->rset_, tl->wset_, lower, upper,
                             tl->lower_, tl->upper_);
        // dli->release(dli->commit_trans_lowup_mutex_);
#else
      res = check_conflict(txn, rset, wset, tl->rset_, tl->wset_);
#endif
      // dli->release(dli->commit_trans_rset_mutex_);
      if (res) {
        rc = Abort;
        break;
      }
    }
  }
#if CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3
  if (lower >= upper) rc = Abort;
#endif
  if (rc == RCOK && final_validate) {
#if CC_ALG != DLI_DTA && CC_ALG != DLI_DTA2 && CC_ALG != DLI_DTA3
    txn->is_abort = &TSNode<Dli::ValidatedTxn>::push_front(dli->validated_txns_, std::move(rset), std::move(wset), ts)->is_abort_;
#else
    txn->is_abort = &TSNode<Dli::ValidatedTxn>::push_front(dli->validated_txns_, std::move(rset), std::move(wset), ts, lower, upper)->is_abort_;
    dta_time_table.set_state(tid, txnid, DTA_VALIDATED);
    dta_time_table.set_lower(tid, txnid, lower);
    dta_time_table.set_upper(tid, txnid, upper);
  } else {
    dta_time_table.set_state(tid, txnid, DTA_ABORTED);
#endif
  }
  timespan = get_sys_clock() - start_time;
  txn->txn_stats.cc_time += timespan;
  txn->txn_stats.cc_time_short += timespan;
  start_time += timespan;

#endif
  return rc;
}

void Dli::get_rw_set(TxnManager* txn, Dli::RWSet& rset,
                     Dli::RWSet& wset) {
  uint64_t len = txn->get_access_cnt();
  for (uint64_t i = 0; i < len; i++) {
    if (txn->get_access_type(i) == WR)
      wset.emplace(txn->get_access_original_row(i), txn->get_access_version(i));
    else
      rset.emplace(txn->get_access_original_row(i), txn->get_access_version(i));
  }
#if WORKLOAD == TPCC
  for (const auto& i : wset) rset[i.first] = i.second;
#endif
}

void Dli::init() {
  // pthread_mutex_init(&validated_trans_mutex_, NULL);
  // pthread_mutex_init(&commit_trans_rset_mutex_, NULL);
  // pthread_mutex_init(&commit_trans_wset_mutex_, NULL);
  // pthread_mutex_init(&commit_trans_lowup_mutex_, NULL);
  pthread_mutex_init(&mtx_, NULL);
}

RC Dli::validate(TxnManager* txn, const bool final_validate) {
  RC rc = RCOK;
  uint64_t starttime = get_sys_clock();
  txnid_t tid = txn->get_thd_id();
  rc = validate_main(txn, this, final_validate);
  INC_STATS(tid, dli_mvcc_occ_validate_time, get_sys_clock() - starttime);
  if (rc == RCOK) {
    INC_STATS(tid, dli_mvcc_occ_check_cnt, 1);
  } else {
    INC_STATS(tid, dli_mvcc_occ_abort_check_cnt, 1);
  }
  return rc;
}

void Dli::finish_trans(RC rc, TxnManager* txn) {
#if CC_ALG == DLI_MVCC || CC_ALG == DLI_MVCC_OCC
  if (rc != RCOK && txn->is_abort != nullptr) {
    txn->is_abort->store(true);
  }
#endif
}
#if CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3
RC Dli::find_bound(TxnManager* txn) {
  RC rc = RCOK;
  txnid_t tid = txn->get_thd_id();
  txnid_t txnid = txn->get_txn_id();
  uint64_t lower = dta_time_table.get_lower(tid, txnid);
  uint64_t upper = dta_time_table.get_upper(tid, txnid);
  if (lower >= upper) {
    dta_time_table.set_state(tid, txnid, DTA_VALIDATED);
    rc = Abort;
  } else {
    dta_time_table.set_state(tid, txnid, DTA_COMMITTED);
    txn->commit_timestamp = lower;
  }
  DEBUG("DTA Bound %ld: %d [%lu,%lu] %lu\n", tid, rc, lower, upper, txn->commit_timestamp);
  return rc;
}
#endif
void Dli::latch() { pthread_mutex_lock(&mtx_); }
void Dli::release() { pthread_mutex_unlock(&mtx_); }
