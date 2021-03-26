#ifndef _ROW_UNIFIED_H_
#define _ROW_UNIFIED_H_

#include "../system/global.h"
#include "../config.h"
#include "unified_util.h"

#if IS_GENERIC_ALG

class row_t;

template <int ALG>
class Row_unified
{
  public:
    void init(row_t* orig_row);
    RC read(TxnManager& txn);
    RC prewrite(TxnManager& txn);
    void write(row_t* ver_row, TxnManager& txn);
    void revoke(row_t* ver_row, TxnManager& txn);

  private:
    row_t* orig_row_;
    std::unique_ptr<UniRowManager<ALG>> row_man_;
};

// prevent cycle include
#include "../system/txn.h"
#include "../storage/row.h"

template <int ALG>
void Row_unified<ALG>::init(row_t* orig_row) {
    orig_row_ = orig_row;
    row_man_ = std::make_unique<UniRowManager<ALG>>(orig_row->get_row_id(), orig_row);
}

template <int ALG>
RC Row_unified<ALG>::read(TxnManager& txn) {
    assert(row_man_ != nullptr);
    txn.cur_row = mem_allocator.construct<row_t>();
    txn.cur_row->init(orig_row_->get_table(), orig_row_->get_part_id());
    const std::optional<row_t*> read_ver_row = row_man_->Read(*txn.uni_txn_man_);
    if (read_ver_row.has_value()) {
        txn.cur_row->copy(*read_ver_row);
        return RCOK;
    } else {
        return Abort;
    }
}

// TODO: Here is a memory leak. We alloc the row_t outside algorithm but not free it because only algorithm
// knows when should row_t free.
template <int ALG>
RC Row_unified<ALG>::prewrite(TxnManager& txn) {
    assert(row_man_ != nullptr);
    txn.cur_row = mem_allocator.construct<row_t>();
    txn.cur_row->init(orig_row_->get_table(), orig_row_->get_part_id());
    return row_man_->Prewrite(txn.cur_row, *txn.uni_txn_man_) ? RCOK : Abort;
}

template <int ALG>
void Row_unified<ALG>::write(row_t* ver_row, TxnManager& txn) {
    row_man_->Write(ver_row, *txn.uni_txn_man_);
}

template <int ALG>
void Row_unified<ALG>::revoke(row_t* ver_row, TxnManager& txn) {
    row_man_->Revoke(ver_row, *txn.uni_txn_man_);
}

#endif
#endif
