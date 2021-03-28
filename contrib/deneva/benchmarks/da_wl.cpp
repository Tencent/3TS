#include "da.h"
#include "da_const.h"
#include "global.h"
#include "helper.h"
#include "index_btree.h"
#include "index_hash.h"
#include "mem_alloc.h"
#include "query.h"
#include "row.h"
#include "table.h"
#include "thread.h"
#include "txn.h"
#include "wl.h"
#include <string>

RC DAWorkload::init() {
  Workload::init();
  char *cpath = getenv("SCHEMA_PATH");
  string path;
  if (cpath == NULL)
    path = "./benchmarks/";
  else {
    path = string(cpath);
  }
  path += "da_schema.txt";
  cout << "reading schema file: " << path << endl;
  delivering = new bool *[g_num_wh + 1];
  for (UInt32 wid = 1; wid <= g_num_wh; wid++)
    delivering[wid] = (bool *)mem_allocator.alloc(CL_SIZE);
  printf("Initializing schema... ");
  fflush(stdout);
  init_schema(path.c_str());
  printf("Done\n");
  printf("Initializing table... ");
  fflush(stdout);
  init_table();
  printf("Done\n");
  fflush(stdout);
  nextstate=0;
  return RCOK;
}

RC DAWorkload::init_schema(const char *schema_file) {
  Workload::init_schema(schema_file);
  printf("base Workload init_schema over");
  t_datab = tables["DAtab"];
  i_datab = indexes["DAtab_IDX"];
  return RCOK;
}

RC DAWorkload::init_table() {
  //pthread_t *p_thds = new pthread_t[g_init_parallelism - 1];
  DA_thr_args *tt = new DA_thr_args[g_init_parallelism];
  for (UInt32 i = 0; i < g_init_parallelism; i++) {
    tt[i].wl = this;
    tt[i].id = i;
  }
  // DA table
  threadInitDAtab(&tt[0]);
  nextstate=0;
  printf("DAtab Done\n");
  return RCOK;
}

void DAWorkload::init_tab_DAtab() {
  row_t *row;
  for (int64_t i = 0; i < ITEM_CNT; i++) {
    uint64_t row_id = i;
    t_datab->get_new_row(row, 0, row_id);
    row->set_value(ID, i);
    row->set_value(VALUE, (int64_t)0);
    //insert_row(row, _wl->t_datab);
    index_insert(i_datab, i, row, 0);
  }
}

void DAWorkload::reset_tab_idx()
{
  index_delete_all();
  init_tab_DAtab();
}

void *DAWorkload::threadInitDAtab(void *This) {
  DAWorkload *wl = ((DA_thr_args *)This)->wl;
  wl->init_tab_DAtab();
  return NULL;
}

RC DAWorkload::get_txn_man(TxnManager *&txn_manager) {
  DEBUG_M("DAWorkload::get_txn_man DATxnManager alloc\n");
  txn_manager = (DATxnManager *)mem_allocator.align_alloc(sizeof(DATxnManager));
  new (txn_manager) DATxnManager();
  // txn_manager->init( this);
  return RCOK;
}
