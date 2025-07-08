/* Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 Tencent.  All rights reserved.
 *
 * Author: hongyaozhao@ruc.edu.cn
 *
 */

#include <cinttypes>
#include <iostream>

#ifndef TCP_TS_H
#define TCP_TS_H

class TcpLtsSocket{
private:
    int         s;


    uint64_t    txn;
    char        *req;
    char        *tcpres;
    char        *buffer;
    int         result;
    int         sendresult;
    int         errorno;
    int         ind;
public:
    void        init();
    int         connectTo(const char *ip, uint16_t port);
    uint64_t    getTimestamp();
    int         closeConnection();
};

#endif
