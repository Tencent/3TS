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

#ifndef _PPS_H_
#define _PPS_H_

#include "wl.h"
#include "txn.h"
#include "query.h"
#include "row.h"

class PPSQuery;
class PPSQueryMessage;
struct Item_no;

class table_t;
class INDEX;
class PPSQuery;
enum PPSRemTxnType {
  PPS_GETPART_S=0,
  PPS_GETPART0,
  PPS_GETPART1,
  PPS_GETPRODUCT_S,
  PPS_GETPRODUCT0,
  PPS_GETPRODUCT1,
  PPS_GETSUPPLIER_S,
  PPS_GETSUPPLIER0,
  PPS_GETSUPPLIER1,
  PPS_GETPARTBYPRODUCT_S,
  PPS_GETPARTBYPRODUCT0,
  PPS_GETPARTBYPRODUCT1,
  PPS_GETPARTBYPRODUCT2,
  PPS_GETPARTBYPRODUCT3,
  PPS_GETPARTBYPRODUCT4,
  PPS_GETPARTBYPRODUCT5,
  PPS_GETPARTBYSUPPLIER_S,
  PPS_GETPARTBYSUPPLIER0,
  PPS_GETPARTBYSUPPLIER1,
  PPS_GETPARTBYSUPPLIER2,
  PPS_GETPARTBYSUPPLIER3,
  PPS_GETPARTBYSUPPLIER4,
  PPS_GETPARTBYSUPPLIER5,
  PPS_ORDERPRODUCT_S,
  PPS_ORDERPRODUCT0,
  PPS_ORDERPRODUCT1,
  PPS_ORDERPRODUCT2,
  PPS_ORDERPRODUCT3,
  PPS_ORDERPRODUCT4,
  PPS_ORDERPRODUCT5,
  PPS_UPDATEPRODUCTPART_S,
  PPS_UPDATEPRODUCTPART0,
  PPS_UPDATEPRODUCTPART1,
  PPS_UPDATEPART_S,
  PPS_UPDATEPART0,
  PPS_UPDATEPART1,
  PPS_FIN,
  PPS_RDONE
};

class PPSWorkload : public Workload {
public:
    RC init();
    RC init_table();
    RC init_schema(const char * schema_file);
    RC get_txn_man(TxnManager *& txn_manager);
    table_t *         t_suppliers;
    table_t *         t_products;
    table_t *         t_parts;
    table_t *         t_supplies;
    table_t *         t_uses;

    INDEX *     i_parts;
    INDEX *     i_products;
    INDEX *     i_suppliers;
    INDEX *     i_supplies;
    INDEX *     i_uses;

private:
    void init_tab_suppliers();
    void init_tab_products();
    void init_tab_parts();
    void init_tab_supplies();
    void init_tab_uses();

    static void * threadInitSuppliers(void * This);
    static void * threadInitProducts(void * This);
    static void * threadInitParts(void * This);
    static void * threadInitSupplies(void * This);
    static void * threadInitUses(void * This);
};

struct pps_thr_args{
    PPSWorkload * wl;
    UInt32 id;
    UInt32 tot;
};

class PPSTxnManager : public TxnManager {
public:
    void init(uint64_t thd_id, Workload * h_wl);
  void reset();
  RC acquire_locks();
    RC run_txn();
    RC run_txn_post_wait();
    RC run_calvin_txn();
  RC run_pps_phase2();
  RC run_pps_phase5();
    PPSRemTxnType state;
  void copy_remote_items(PPSQueryMessage * msg);
private:
    PPSWorkload * _wl;
    volatile RC _rc;
  row_t * row;

  uint64_t parts_processed_count;

  void next_pps_state();
  RC run_txn_state();
  bool is_done();
  bool is_local_item(uint64_t idx);
  RC send_remote_request();

inline void getThreeFields(row_t *& r_local);
inline void getAllFields(row_t *& r_local);
inline RC run_getpart_0(uint64_t part_key, row_t *& r_local);
inline RC run_getpart_1(row_t *& r_local);
inline RC run_getproduct_0(uint64_t product_key, row_t *& r_local);
inline RC run_getproduct_1(row_t *& r_local);
inline RC run_getsupplier_0(uint64_t supplier_key, row_t *& r_local);
inline RC run_getsupplier_1(row_t *& r_local);
inline RC run_getpartsbyproduct_0(uint64_t product_key, row_t *& r_local);
inline RC run_getpartsbyproduct_1(row_t *& r_local);
inline RC run_getpartsbyproduct_2(uint64_t product_key, row_t *& r_local);
inline RC run_getpartsbyproduct_3(uint64_t &part_key, row_t *& r_local);
inline RC run_getpartsbyproduct_4(uint64_t part_key, row_t *& r_local);
inline RC run_getpartsbyproduct_5(row_t *& r_local);
inline RC run_getpartsbysupplier_0(uint64_t supplier_key, row_t *& r_local);
inline RC run_getpartsbysupplier_1(row_t *& r_local);
inline RC run_getpartsbysupplier_2(uint64_t supplier_key, row_t *& r_local);
inline RC run_getpartsbysupplier_3(uint64_t &part_key, row_t *& r_local);
inline RC run_getpartsbysupplier_4(uint64_t part_key, row_t *& r_local);
inline RC run_getpartsbysupplier_5(row_t *& r_local);
inline RC run_orderproduct_0(uint64_t product_key, row_t *& r_local);
inline RC run_orderproduct_1(row_t *& r_local);
inline RC run_orderproduct_2(uint64_t product_key, row_t *& r_local);
inline RC run_orderproduct_3(uint64_t &part_key, row_t *& r_local);
inline RC run_orderproduct_4(uint64_t part_key, row_t *& r_local);
inline RC run_orderproduct_5(row_t *& r_local);
inline RC run_updateproductpart_0(uint64_t product_key, row_t *& r_local);
inline RC run_updateproductpart_1(uint64_t part_key, row_t *& r_local);
inline RC run_updatepart_0(uint64_t part_key, row_t *& r_local);
inline RC run_updatepart_1(row_t *& r_local);
};

#endif
