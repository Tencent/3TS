/* Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: hongyaozhao@ruc.edu.cn
 *
 */

#include "libtcpforcpp.h"

#include <unistd.h>

#include <arpa/inet.h>
#include <errno.h>
#include <sys/socket.h>

#include <iostream>
#include <string>

#include "manager.h"
#include "ltsrpc.pb.h"

const int headerLen = 2;
const int minSendLength = 24;

void TcpLtsSocket::init() {
    txn = 12345678;
    req = (char *)malloc(minSendLength);
    tcpres = (char *)malloc(minSendLength);
    buffer = (char *)malloc(18);
    ind = 0;
    result = 0;
    sendresult = 0;
    errorno = 0;
}

int TcpLtsSocket::connectTo(const char *ip, uint16_t port)
{
    s = socket(AF_INET, SOCK_STREAM, 0);
    if (s < 0)
    {
        return -1;
    }
    struct sockaddr_in addr;
    bzero(&addr, sizeof(struct sockaddr_in));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = inet_addr(ip);
    if (connect(s, (sockaddr *)&addr, sizeof(addr)) < 0)
    {
        return -1;
    }
    struct timeval tv;
    tv.tv_sec = 1;
    tv.tv_usec = 0;
    setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, (const char *)&tv, sizeof(struct timeval));

    return 0;
}

uint64_t TcpLtsSocket::getTimestamp()
{
    if (s == -1)
    {
        while (connectTo(LTS_TCP_IP, LTS_TCP_PORT));
    }
    ltsrpc::GetTxnTimestampCtx lts;

    lts.set_txn_id(txn);

    std::string message;
    lts.SerializeToString(&message);
    auto size = (uint16_t)message.size();
    uint16_t cpsize = htons(size);

    memcpy(req, (void *)&cpsize, sizeof(uint16_t));
    memcpy(req + headerLen, message.data(), size);
    memset(req + headerLen + size, 0, minSendLength - headerLen - size);
    uint64_t start_ts = get_sys_clock();
resend:
    sendresult = send(s, req, minSendLength, 0);

    ind = 0;

    for (;;)
    {
    rerecv:
        result = recv(s, buffer, 18, 0);
        if (result == -1)
        {
            errorno = errno;
            switch (errorno)
            {
            case 104:
                break;
            case EINTR:
            case EAGAIN:
                goto rerecv;
            default:
                break;
            }
            closeConnection();
            while (connectTo(LTS_TCP_IP, LTS_TCP_PORT))
                ;
            goto resend;
        }
        if (ind + result > 18)
        {
            ind = 0;
        }
        memcpy(tcpres + ind, buffer, result);
        ind += result;
        if (ind == 18)
        {
            ind = 0;
            break;
        }
    }
    // free(buffer);

    uint16_t olen = *(uint16_t *)tcpres;
    uint16_t respLen = ntohs(olen);
    lts.ParseFromArray(tcpres + 2, respLen);
    uint64_t end_ts = get_sys_clock();
    DEBUG("Tcp get timestamp %ld by id %ld latency %ld\n",lts.txn_ts(), txn, end_ts-start_ts);
    // DEBUG("Tcp get timestamp %ld by id %ld\n",lts.txn_ts(),txn);
    return lts.txn_ts();
}

int TcpLtsSocket::closeConnection()
{
    if (s == -1)
        return 0;
    int re = close(s);
    s = -1;
    return re;
}
