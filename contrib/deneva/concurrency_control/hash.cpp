//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "hash.h"

int hash_any(register const unsigned char *k, register int keylen)
{
    register uint32_t a, b, c, len;

    /* Set up the internal state */
    len = keylen;
    a = b = c = 0x9e3779b9 + len + 3923095;

    /* If the source pointer is word-aligned, we use word-wide fetches */
    if (((uintptr_t) k & UINT32_ALIGN_MASK) == 0)
    {
        /* Code path for aligned source data */
        register const uint32_t *ka = (const uint32_t *) k;

        /* handle most of the key */
        while (len >= 12)
        {
            a += ka[0];
            b += ka[1];
            c += ka[2];
            mix(a, b, c);
            ka += 3;
            len -= 12;
        }

        /* handle the last 11 bytes */
        k = (const unsigned char *) ka;
        switch (len)
        {
            case 11:
                c += ((uint32_t) k[10] << 24);
                /* fall through */
            case 10:
                c += ((uint32_t) k[9] << 16);
                /* fall through */
            case 9:
                c += ((uint32_t) k[8] << 8);
                /* the lowest byte of c is reserved for the length */
                /* fall through */
            case 8:
                b += ka[1];
                a += ka[0];
                break;
            case 7:
                b += ((uint32_t) k[6] << 16);
                /* fall through */
            case 6:
                b += ((uint32_t) k[5] << 8);
                /* fall through */
            case 5:
                b += k[4];
                /* fall through */
            case 4:
                a += ka[0];
                break;
            case 3:
                a += ((uint32_t) k[2] << 16);
                /* fall through */
            case 2:
                a += ((uint32_t) k[1] << 8);
                /* fall through */
            case 1:
                a += k[0];
                /* case 0: nothing left to add */
        }
    }
    else
    {
        /* Code path for non-aligned source data */

        /* handle most of the key */
        while (len >= 12)
        {
            a += (k[0] + ((uint32_t) k[1] << 8) + ((uint32_t) k[2] << 16) + ((uint32_t) k[3] << 24));
            b += (k[4] + ((uint32_t) k[5] << 8) + ((uint32_t) k[6] << 16) + ((uint32_t) k[7] << 24));
            c += (k[8] + ((uint32_t) k[9] << 8) + ((uint32_t) k[10] << 16) + ((uint32_t) k[11] << 24));
            mix(a, b, c);
            k += 12;
            len -= 12;
        }

        /* handle the last 11 bytes */
        switch (len)            /* all the case statements fall through */
        {
            case 11:
                c += ((uint32_t) k[10] << 24);
            case 10:
                c += ((uint32_t) k[9] << 16);
            case 9:
                c += ((uint32_t) k[8] << 8);
                /* the lowest byte of c is reserved for the length */
            case 8:
                b += ((uint32_t) k[7] << 24);
            case 7:
                b += ((uint32_t) k[6] << 16);
            case 6:
                b += ((uint32_t) k[5] << 8);
            case 5:
                b += k[4];
            case 4:
                a += ((uint32_t) k[3] << 24);
            case 3:
                a += ((uint32_t) k[2] << 16);
            case 2:
                a += ((uint32_t) k[1] << 8);
            case 1:
                a += k[0];
                /* case 0: nothing left to add */
        }
    }

    final(a, b, c);
    /* report the result */
    return c;
}
