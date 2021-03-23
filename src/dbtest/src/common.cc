/*
 * Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: farrisli (farrisli@tencent.com)
 *
 */
#include "common.h"
std::string dash(10, '-');
int blank_base = 40;

void TrimSpace(std::string& str) {
    auto itor = remove_if(str.begin(), str.end(), ::isspace);
    str.erase(itor, str.end());
}

void TrimQuo(std::string& str) {
    str.erase(std::remove(str.begin(), str.end(), '\"'), str.end());
}

void split(const std::string& str, std::vector<std::string>& tokens, const std::string delim) {
    tokens.clear();
    auto start = str.find_first_not_of(delim, 0);
    auto position = str.find_first_of(delim, start);
    while (position != std::string::npos || start != std::string::npos) {
        tokens.emplace_back(std::move(str.substr(start, position - start)));
        start = str.find_first_not_of(delim, position);
        position = str.find_first_of(delim, start);
    }
}
