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
#include "manager.h"
#include "http.h"
#include "libtcpforcpp.h"
#include <stdlib.h>
#include <unistd.h>
#include <curl/curl.h>
#include <fcntl.h>
#include <unistd.h>
#include "mem_alloc.h"
#include "helper.h"

static uint64_t stouint64(char *ptr)
{
    uint64_t num = 0;
    int i = 0;
    while (ptr[i])
    {
        num = num * 10 + (ptr[i++] - '0');
    }
    return num;
}

size_t WriteTimeStampCallBack(void *ptr, size_t size, size_t nmemb, void *data)
{
    size_t realsize = size * nmemb;
    uint64_t *ts = (uint64_t *)data;
    *ts = stouint64((char *)ptr);
    return realsize;
}

static char curlip[100];

uint64_t CurlGetTimeStamp(void)
{
    CURL *curl;
    CURLcode res;

    curl = curl_easy_init();
    if (!curl)
    {
        fprintf(stderr, "curl_easy_init() error");
        return 1;
    }
    uint64_t ts;
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &ts);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteTimeStampCallBack);

    snprintf(curlip, sizeof(curlip), "http://%s:%d/lts-cluster/api/ts/1", LTS_TCP_IP, LTS_TCP_PORT);
    curl_easy_setopt(curl, CURLOPT_URL, curlip);
    res = curl_easy_perform(curl);
    if (res != CURLE_OK)
    {
        fprintf(stderr, "curl_easy_perform() error: %s\n", curl_easy_strerror(res));
    }
    curl_easy_cleanup(curl);
    return ts;
}

void TcpTimestamp::init(int all_thd_num)
{
    socket = (TcpLtsSocket*)mem_allocator.align_alloc(sizeof(TcpLtsSocket) * all_thd_num);
    socket_num = all_thd_num;
    for (int i = 0; i < all_thd_num; i ++) {
        socket[i].init();
        ConnectToLts(i);
    }

}

void TcpTimestamp::ConnectToLts(uint64_t thd_id)
{
    int result = 0;
    for (int retry = 0; retry < 10; retry++)
    {
        result = socket[thd_id].connectTo(LTS_TCP_IP, LTS_TCP_PORT);
        if (result == 0)
            break;
    }
    if (result != 0)
    {
        printf("\nLTS: connection failed");
    }
}

void TcpTimestamp::CloseToLts(uint64_t thd_id)
{
    int result = 0;
    result = socket[thd_id].closeConnection();
    if (result != 0)
    {
        printf("\nLTS: close failed");
    }
}

uint64_t TcpTimestamp::TcpGetTimeStamp(uint64_t thd_id)
{
    uint64_t start_ts = get_sys_clock();
    uint64_t ts = 0;
    ts = socket[thd_id].getTimestamp();

    if (ts == (uint64_t)-1)
    {
        //core_dump();
    }
    uint64_t end_ts = get_sys_clock();
    DEBUG("Tcp get timestamp %ld, latency %ld\n",ts, end_ts-start_ts);
    return ts;
}
