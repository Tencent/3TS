#ifndef ROW_SI_H
#define ROW_SI_H
#include <atomic>
#include <unordered_set>
#include <vector>
#include <mutex>

#include "../storage/row.h"
#include "../system/global.h"

class TxnManager;
class row_t;

struct SIEntry {
    SIEntry(row_t* const row, const ts_t w_ts, const ts_t commit_ts)
            : row(row), w_ts(w_ts), commit_ts(commit_ts) {}
    SIEntry(const SIEntry&) = delete;
    SIEntry(SIEntry&&) = default;
    row_t* row;
    ts_t w_ts;
    ts_t commit_ts;
    std::mutex mutex;
};

template <typename T>
class TSNode : public T {
  public:
    using T::T;

    template <typename ...Args>
    static TSNode<T>* push_front(std::atomic<TSNode<T>*>& head, Args&&... args) {
        TSNode<T>* new_head = new TSNode<T>(std::forward<Args>(args)...);
        new_head->next_ = head.load(std::memory_order_relaxed);
        while (!head.compare_exchange_weak(new_head->next_, new_head, std::memory_order_release, std::memory_order_relaxed));
        return new_head;
    }

    TSNode* next_ = nullptr;
};

class Row_si {
  public:
    void init(row_t* row);
    RC access(TxnManager* txn, TsType type, uint64_t& version);
    uint64_t write(row_t* data, TxnManager* txn, access_t type);
    void latch();
    void release();
    std::atomic<uint64_t> w_trans;
    std::atomic<ts_t> timestamp_last_write;

  private:
    pthread_mutex_t* latch_;
    row_t* row_;
    std::atomic<TSNode<SIEntry>*> versions_;
};
#endif
