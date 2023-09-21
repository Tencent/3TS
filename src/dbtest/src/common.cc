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

/**
 * Removes all trailing whitespace characters from the given string str.
 * 
 * This function aims to remove all whitespace characters such as spaces, tabs, enters, etc., from the given string str.
 * It utilizes the remove_if function, which moves all non-whitespace characters to the front of the string and then returns an iterator pointing to the first removed element.
 * Subsequently, the erase function is used to delete all characters starting from that iterator to the end of the string.
 * 
 * @param str The string from which trailing whitespace characters are to be removed.
 */
void TrimSpace(std::string& str) {
    auto itor = remove_if(str.begin(), str.end(), ::isspace);
    str.erase(itor, str.end());
}

/**
 * Removes all double quote (") characters from the given string str.
 * 
 * This function is designed to remove all occurrences of the double quote character from the specified string str.
 * 
 * @param str The string from which the double quote characters are to be removed.
 */
void TrimQuo(std::string& str) {
    str.erase(std::remove(str.begin(), str.end(), '\"'), str.end());
}

/**
 * Splits the given string str by the delimiter delim and stores the results in the tokens vector.
 * 
 * This function aims to split the specified string str based on the delimiter delim. The resulting substrings are then 
 * stored in the tokens vector. Initially, the tokens vector is cleared to ensure no pre-existing data remains.
 * To achieve the splitting, the function uses two iterators, start and position, which cycle through the string 
 * to identify and extract substrings separated by the delimiter. These substrings are then appended to the tokens vector.
 * 
 * @param str The string to be split.
 * @param tokens A vector to hold the split substrings.
 * @param delim The delimiter used for splitting the string.
 */
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
