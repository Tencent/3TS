/* Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: hongyaozhao@ruc.edu.cn
 *
 */

#ifndef HTTP_TS_H
#define HTTP_TS_H

#include <stdlib.h>
#include <unistd.h>

#include <curl/curl.h>

#include <cinttypes>
#include "libtcpforcpp.h"

uint64_t CurlGetTimeStamp(void);
uint64_t TcpGetTimeStamp(void);

void ConnectToLts(void);
void CloseToLts(void);

class TcpTimestamp {
public:
  void init(int all_thd_num);
  void ConnectToLts(uint64_t thd_id);
  void CloseToLts(uint64_t thd_id);
  uint64_t TcpGetTimeStamp(uint64_t thd_id);
private:
  TcpLtsSocket * socket;
  int socket_num;
  // pthread_mutex_t ** lock_;
};

#endif
