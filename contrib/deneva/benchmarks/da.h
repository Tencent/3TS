#ifndef _DA_H_
#define _DA_H_
#include "config.h"
#include "query.h"
#include "row.h"
#include "txn.h"
#include "wl.h"
#include "creator.h"
#include "da_query.h"
#include <optional>

class DAQuery;
class DAQueryMessage;
struct Item_no;

class table_t;
class INDEX;
class DAQuery;

extern std::string DA_history_mem;
extern std::vector<Message*> DA_delayed_operations;
extern bool abort_history;
extern ofstream commit_file;
extern ofstream abort_file;
template <typename Txn>
extern std::optional<ttts::Path<Txn>> g_da_cycle;

class DAWorkload : public Workload {
  public:
    RC init();
    RC init_table();
    RC init_schema(const char* schema_file);
    RC get_txn_man(TxnManager*& txn_manager);
    void reset_tab_idx();
    table_t* t_datab;
    uint64_t nextstate;
    INDEX* i_datab;
    bool** delivering;

  private:
    //void init_tab_DAtab(int id, uint64_t w_id);
    void init_tab_DAtab();
    static void* threadInitDAtab(void* This);
};

struct DA_thr_args {
    DAWorkload* wl;
    UInt32 id;
    UInt32 tot;
};

class DATxnManager : public TxnManager {
  public:
    void init(uint64_t thd_id, Workload* h_wl);
    void reset();
    RC acquire_locks();
    RC run_txn();
    RC run_txn_post_wait();
    RC run_calvin_txn();

    void copy_remote_items(DAQueryMessage* msg);
    void set_not_waiting() { is_waiting_ = false; }

  private:
    DAWorkload* _wl;
    volatile RC _rc;
    row_t* row;
    bool is_waiting_;
    std::vector<DAQuery> skip_queries_;

    uint64_t next_item_id;

    bool is_done();
    bool is_local_item(uint64_t idx);
    RC send_remote_request();
    RC run_delivery(DAQuery* query);
    RC process_query(const DAQuery* const da_query);
};
#endif
