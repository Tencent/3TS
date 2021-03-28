
#pragma once

#ifndef KEY_RANGE_HASH
#define KEY_RANGE_HASH

#include <limits>
#include <string>
#include <vector>
#include <map>

#define UINT32_ALIGN_MASK (sizeof(uint32_t) - 1)
#define rot(x, k) (((x) << (k)) | ((x) >> (32 - (k))))
#define mix(a, b, c) \
{                    \
    a -= c;          \
    a ^= rot(c, 4);  \
    c += b;          \
    b -= a;          \
    b ^= rot(a, 6);  \
    a += c;          \
    c -= b;          \
    c ^= rot(b, 8);  \
    b += a;          \
    a -= c;          \
    a ^= rot(c, 16); \
    c += b;          \
    b -= a;          \
    b ^= rot(a, 19); \
    a += c;          \
    c -= b;          \
    c ^= rot(b, 4);  \
    b += a;          \
}

#define final(a, b, c) \
{                      \
    c ^= b;            \
    c -= rot(b, 14);   \
    a ^= c;            \
    a -= rot(c, 11);   \
    b ^= a;            \
    b -= rot(a, 25);   \
    c ^= b;            \
    c -= rot(b, 16);   \
    a ^= c;            \
    a -= rot(c, 4);    \
    b ^= a;            \
    b -= rot(a, 14);   \
    c ^= b;            \
    c -= rot(b, 24);   \
}

int hash_any(const unsigned char *k, int keylen);

#endif
