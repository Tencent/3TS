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

#include "global.h"
#include "index_hash.h"
#include "mem_alloc.h"
#include "row.h"

RC IndexHash::init(uint64_t bucket_cnt) {
    _bucket_cnt = bucket_cnt;
    _bucket_cnt_per_part = bucket_cnt;

    _buckets = new BucketHeader * [1];
    _buckets[0] = (BucketHeader *) mem_allocator.alloc(sizeof(BucketHeader) * _bucket_cnt_per_part);
    uint64_t buckets_init_cnt = 0;
    for (UInt32 n = 0; n < _bucket_cnt_per_part; n ++) {
                _buckets[0][n].init();
        ++buckets_init_cnt;
    }
    printf("Index init with %ld buckets\n",buckets_init_cnt);
    return RCOK;
}

RC IndexHash::init(int part_cnt, table_t *table, uint64_t bucket_cnt) {
    init(bucket_cnt);
    this->table = table;
    return RCOK;
}

void IndexHash::index_delete() {
    for (UInt32 n = 0; n < _bucket_cnt_per_part; n ++) {
        _buckets[0][n].delete_bucket();
    }
    mem_allocator.free(_buckets[0],sizeof(BucketHeader) * _bucket_cnt_per_part);
    delete _buckets;
}

void IndexHash::index_reset() {
    for (UInt32 n = 0; n < _bucket_cnt_per_part; n ++) {
        _buckets[0][n].delete_bucket();
    }
}

bool IndexHash::index_exist(idx_key_t key) {
    assert(false);
}

void
IndexHash::get_latch(BucketHeader * bucket) {
    while (!ATOM_CAS(bucket->locked, false, true)) {}
}

void
IndexHash::release_latch(BucketHeader * bucket) {
    bool ok = ATOM_CAS(bucket->locked, true, false);
    assert(ok);
}


RC IndexHash::index_insert(idx_key_t key, itemid_t * item, int part_id) {
    RC rc = RCOK;
    uint64_t bkt_idx = hash(key);
    assert(bkt_idx < _bucket_cnt_per_part);
    //BucketHeader * cur_bkt = &_buckets[part_id][bkt_idx];
    BucketHeader * cur_bkt = &_buckets[0][bkt_idx];
    // 1. get the ex latch
    get_latch(cur_bkt);

    // 2. update the latch list
    cur_bkt->insert_item(key, item, part_id);

    // 3. release the latch
    release_latch(cur_bkt);//delete 
    return rc;
}
RC IndexHash::index_insert_nonunique(idx_key_t key, itemid_t * item, int part_id) {
    RC rc = RCOK;
    uint64_t bkt_idx = hash(key);
    assert(bkt_idx < _bucket_cnt_per_part);
    //BucketHeader * cur_bkt = &_buckets[part_id][bkt_idx];
    BucketHeader * cur_bkt = &_buckets[0][bkt_idx];
    // 1. get the ex latch
    get_latch(cur_bkt);

    // 2. update the latch list
    cur_bkt->insert_item_nonunique(key, item, part_id);

    // 3. release the latch
    release_latch(cur_bkt);
    return rc;
}

RC IndexHash::index_read(idx_key_t key, itemid_t * &item, int part_id) {
    uint64_t bkt_idx = hash(key);
    assert(bkt_idx < _bucket_cnt_per_part);
    //BucketHeader * cur_bkt = &_buckets[part_id][bkt_idx];
    BucketHeader * cur_bkt = &_buckets[0][bkt_idx];
    RC rc = RCOK;
    // 1. get the sh latch

    cur_bkt->read_item(key, item);

    // 3. release the latch
    return rc;

}

RC IndexHash::index_read(idx_key_t key, int count, itemid_t * &item, int part_id) {
    uint64_t bkt_idx = hash(key);
    assert(bkt_idx < _bucket_cnt_per_part);
    //BucketHeader * cur_bkt = &_buckets[part_id][bkt_idx];
    BucketHeader * cur_bkt = &_buckets[0][bkt_idx];
    RC rc = RCOK;
    // 1. get the sh latch

    cur_bkt->read_item(key, count, item);

    // 3. release the latch
    return rc;

}


RC IndexHash::index_read(idx_key_t key, itemid_t * &item,
                        int part_id, int thd_id) {
    uint64_t bkt_idx = hash(key);
    assert(bkt_idx < _bucket_cnt_per_part);
    //BucketHeader * cur_bkt = &_buckets[part_id][bkt_idx];
    BucketHeader * cur_bkt = &_buckets[0][bkt_idx];
    RC rc = RCOK;
    // 1. get the sh latch


    cur_bkt->read_item(key, item);

    // 3. release the latch
    return rc;
}

/************** BucketHeader Operations ******************/

void BucketHeader::init() {
    node_cnt = 0;
    first_node = NULL;
    locked = false;
}

void BucketHeader::delete_bucket() {
    BucketNode * cur_node = first_node;
    while (cur_node != NULL) {
    ((row_t *)cur_node->items->location)->free_row();
        cur_node = cur_node->next;
    }
    first_node=NULL;
}


void BucketHeader::insert_item(idx_key_t key,
        itemid_t * item,
        int part_id)
{

    BucketNode * cur_node = first_node;
    BucketNode * prev_node = NULL;
    while (cur_node != NULL) {
        if (cur_node->key == key)
            break;
        prev_node = cur_node;
        cur_node = cur_node->next;
    }
    if (cur_node == NULL) {
        BucketNode * new_node = (BucketNode *)
            mem_allocator.alloc(sizeof(BucketNode));
        new_node->init(key);
        new_node->items = item;
        if (prev_node != NULL) {
            new_node->next = prev_node->next;
            prev_node->next = new_node;
        } else {
            new_node->next = first_node;
            first_node = new_node;
        }
    } else {
        item->next = cur_node->items;
        cur_node->items = item;
    }
}


void BucketHeader::insert_item_nonunique(idx_key_t key,
        itemid_t * item,
        int part_id)
{

    BucketNode * new_node = (BucketNode *)
    mem_allocator.alloc(sizeof(BucketNode));
    new_node->init(key);
    new_node->items = item;
    new_node->next = first_node;
    first_node = new_node;
}

void BucketHeader::read_item(idx_key_t key, itemid_t *&item) {
    BucketNode * cur_node = first_node;
    while (cur_node != NULL) {
    if (cur_node->key == key) break;
        cur_node = cur_node->next;
    }
    M_ASSERT_V(cur_node != NULL, "Key does not exist! %ld\n",key);

    assert(cur_node->key == key);
    item = cur_node->items;
}

void BucketHeader::read_item(idx_key_t key, uint32_t count, itemid_t *&item) {
    BucketNode * cur_node = first_node;
    uint32_t ctr = 0;
    while (cur_node != NULL) {
        if (cur_node->key == key) {
            if (ctr == count) {
                break;
            }
            ++ctr;
        }
        cur_node = cur_node->next;
    }
    if (cur_node == NULL) {
        item = NULL;
        return;
    }
    M_ASSERT_V(cur_node != NULL, "Key does not exist! %ld\n",key);
    assert(cur_node->key == key);
    item = cur_node->items;
}
