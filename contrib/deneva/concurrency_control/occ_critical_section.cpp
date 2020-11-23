/* Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: hongyaozhao@ruc.edu.cn
 *
 */
#include "global.h"
#include "helper.h"
#include "txn.h"
#include "occ_critical_section.h"

void occ_cs::init() {
    sem_init(&cs_semaphore, 0, 1);
}

RC occ_cs::start_critical_section(TxnManager * txn) {
    RC rc = RCOK;
    sem_wait(&cs_semaphore);
    LockEntry *entry = (LockEntry *)mem_allocator.alloc(sizeof(LockEntry));
    entry->txn = txn;
    if (owners == NULL) {
        STACK_PUSH(owners, entry);
        DEBUG("OCC go into critical section %ld\n",txn->get_txn_id());
        goto scs;
    } else if (owners->txn == txn) {
        DEBUG("OCC wait end and go into critical section %ld\n",txn->get_txn_id());
    } else {
        LIST_PUT_TAIL(waiters_head, waiters_tail, entry);
        DEBUG("OCC wait for go into critical section %ld\n",txn->get_txn_id());
        rc = WAIT;
    }
scs:
    sem_post(&cs_semaphore);
    return rc;
}

RC occ_cs::end_critical_section(TxnManager * txn) {
    RC rc = RCOK;
    DEBUG("OCC want to go out from critical section %ld\n",txn->get_txn_id());
    sem_wait(&cs_semaphore);
    // go out from the critical section;
    if (owners->txn != txn) {
        sem_post(&cs_semaphore);
        return rc;
    }
    mem_allocator.free(owners, sizeof(LockEntry));
    owners = NULL;
    DEBUG("OCC go out from critical section %ld\n",txn->get_txn_id());
    LockEntry * entry;
    if (waiters_head) {
        LIST_GET_HEAD(waiters_head, waiters_tail, entry);
        STACK_PUSH(owners, entry);

        txn_table.restart_txn(txn->get_thd_id(), entry->txn->get_txn_id(),
                        entry->txn->get_batch_id());
    }
    sem_post(&cs_semaphore);
    return rc;
}

bool occ_cs::check_critical_section() {
    sem_wait(&cs_semaphore);
    bool exist = owners == NULL;
    sem_post(&cs_semaphore);
    return exist;
}

void occ_cs::set_remote_critical_section(TxnManager * txn) {
    sem_wait(&cs_semaphore);
    LockEntry *entry = (LockEntry *)mem_allocator.alloc(sizeof(LockEntry));
    entry->txn = txn;
    if (owners == NULL) {
        STACK_PUSH(owners, entry);
        DEBUG("OCC remote set critical section %ld\n",txn->get_txn_id());
    } else if (owners->txn == txn) {
        DEBUG("OCC remote has set critical section %ld\n",txn->get_txn_id());
    }
    sem_post(&cs_semaphore);
}

void occ_cs::unset_remote_critical_section(TxnManager * txn) {
    sem_wait(&cs_semaphore);
    mem_allocator.free(owners, sizeof(LockEntry));
    owners = NULL;
    sem_post(&cs_semaphore);
}