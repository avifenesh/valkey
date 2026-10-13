/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "generated_wrappers.hpp"

#include <cstdint>
#include <cstdlib>
#include <cstring>

extern "C" {
uint16_t crc16(const char *buf, int len);
unsigned int keyHashSlot(const char *key, int keylen);
}

/* Bit-at-a-time CRC-16/XMODEM, used as the reference for the table driven crc16(). */
static uint16_t crc16Bitwise(const char *buf, int len) {
    uint16_t crc = 0;
    for (int i = 0; i < len; i++) {
        crc ^= (uint16_t)((unsigned char)buf[i] << 8);
        for (int b = 0; b < 8; b++) {
            crc = (crc & 0x8000) ? (uint16_t)((crc << 1) ^ 0x1021) : (uint16_t)(crc << 1);
        }
    }
    return crc;
}

class Crc16Test : public ::testing::Test {};

TEST_F(Crc16Test, TestCrc16KnownValues) {
    ASSERT_EQ(crc16("", 0), 0);
    ASSERT_EQ(crc16("123456789", 9), 0x31C3);
    ASSERT_EQ(keyHashSlot("123456789", 9), 0x31C3u & 0x3FFF);
    /* Keys from the cluster specification. */
    ASSERT_EQ(keyHashSlot("foo", 3), 12182u);
    ASSERT_EQ(keyHashSlot("bar", 3), 5061u);
}

TEST_F(Crc16Test, TestCrc16MatchesBitwise) {
    char buf[512];
    srand(1234);
    for (size_t i = 0; i < sizeof(buf); i++) buf[i] = (char)(rand() & 0xff);

    /* Cover every tail length and every alignment of the 8 bytes per step loop. */
    for (int len = 0; len <= 300; len++) {
        for (int off = 0; off < 8; off++) {
            ASSERT_EQ(crc16(buf + off, len), crc16Bitwise(buf + off, len)) << "len " << len << " off " << off;
        }
    }
}

TEST_F(Crc16Test, TestKeyHashSlotHashTags) {
    char key[64];
    /* Only the part between the first '{' and the next '}' is hashed. */
    ASSERT_EQ(keyHashSlot("{user1000}.following", 20), keyHashSlot("user1000", 8));
    ASSERT_EQ(keyHashSlot("{user1000}.followers", 20), keyHashSlot("user1000", 8));
    ASSERT_EQ(keyHashSlot("foo{bar}{zap}", 13), keyHashSlot("bar", 3));
    ASSERT_EQ(keyHashSlot("foo{{bar}}zap", 13), keyHashSlot("{bar", 4));
    /* Empty tag, missing '}' or no '{' hash the whole key. */
    ASSERT_EQ(keyHashSlot("foo{}{bar}", 10), crc16("foo{}{bar}", 10) & 0x3FFFu);
    ASSERT_EQ(keyHashSlot("foo{bar", 7), crc16("foo{bar", 7) & 0x3FFFu);
    ASSERT_EQ(keyHashSlot("foo}bar{", 8), crc16("foo}bar{", 8) & 0x3FFFu);
    ASSERT_EQ(keyHashSlot("{", 1), crc16("{", 1) & 0x3FFFu);
    ASSERT_EQ(keyHashSlot("", 0), 0u);
    /* The tag search must stop at keylen. */
    memcpy(key, "abc{def}", 8);
    ASSERT_EQ(keyHashSlot(key, 5), crc16("abc{d", 5) & 0x3FFFu);
    ASSERT_EQ(keyHashSlot(key, 7), crc16("abc{def", 7) & 0x3FFFu);
}
