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
#include <iostream>
#include <fstream>
#include <iomanip>

#include <algorithm>
#include <cstdlib>
#include <cassert>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <string>
#include <vector>
#include <unordered_map>
#include <iterator>
#include <thread>

extern std::string dash;
extern int blank_base;
// trim all space
void TrimSpace(std::string& str);
// trim all quotation mark
void TrimQuo(std::string& str);
// split string with delim
void split(const std::string& str, std::vector<std::string>& tokens, const std::string delim = " ");
