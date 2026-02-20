// Pulled from lookup3.c by Bob Jenkins
#include "hashutil.h"
#include <cstring>

#define rot(x, k) (((x) << (k)) | ((x) >> (32 - (k))))
#define mix(a,b,c)                              \
    {                                           \
        a -= c;  a ^= rot(c, 4);  c += b;       \
        b -= a;  b ^= rot(a, 6);  a += c;       \
        c -= b;  c ^= rot(b, 8);  b += a;       \
        a -= c;  a ^= rot(c,16);  c += b;       \
        b -= a;  b ^= rot(a,19);  a += c;       \
        c -= b;  c ^= rot(b, 4);  b += a;       \
    }

#define final(a,b,c)                            \
    {                                           \
        c ^= b; c -= rot(b,14);                 \
        a ^= c; a -= rot(c,11);                 \
        b ^= a; b -= rot(a,25);                 \
        c ^= b; c -= rot(b,16);                 \
        a ^= c; a -= rot(c,4);                  \
        b ^= a; b -= rot(a,14);                 \
        c ^= b; c -= rot(b,24);                 \
    }
// Assuming little endian
#define HASH_LITTLE_ENDIAN 1

#define get16bits(d) (*((const uint16_t *)(d)))

namespace cuckoofilter {
/*
  hashlittle() -- hash a variable-length key into a 32-bit value
  k       : the key (the unaligned variable-length array of bytes)
  length  : the length of the key, counting by bytes
  initval : can be any 4-byte value
  Returns a 32-bit value.  Every bit of the key affects every bit of
  the return value.  Two keys differing by one or two bits will have
  totally different hash values.

  The best hash table sizes are powers of 2.  There is no need to do
  mod a prime (mod is sooo slow!).  If you need less than 32 bits,
  use a bitmask.  For example, if you need only 10 bits, do
  h = (h & hashmask(10));
  In which case, the hash table should have hashsize(10) elements.

  If you are hashing n strings (uint8_t **)k, do it like this:
  for (i=0, h=0; i<n; ++i) h = hashlittle( k[i], len[i], h);

  By Bob Jenkins, 2006.  bob_jenkins@burtleburtle.net.  You may use this
  code any way you wish, private, educational, or commercial.  It's free.

  Use for hash table lookup, or anything where one collision in 2^^32 is
  acceptable.  Do NOT use for cryptographic purposes.
*/

uint32_t HashUtil::BobHash(const std::string &s, uint32_t seed) {
  return BobHash(s.data(), s.length(), seed);
}

uint32_t HashUtil::BobHash(const void *buf, size_t length, uint32_t seed) {
  uint32_t a, b, c; /* internal state */
  union {
    const void *ptr;
    size_t i;
  } u; /* needed for Mac Powerbook G4 */

  /* Set up the internal state */
  // Is it safe to use key as the initial state setter?
  a = b = c = 0xdeadbeef + ((uint32_t)length) + seed;

  u.ptr = buf;
  if (HASH_LITTLE_ENDIAN && ((u.i & 0x3) == 0)) {
    const uint32_t *k = (const uint32_t *)buf; /* read 32-bit chunks */

    /*------ all but last block: aligned reads and affect 32 bits of (a,b,c) */
    while (length > 12) {
      a += k[0];
      b += k[1];
      c += k[2];
      mix(a, b, c);
      length -= 12;
      k += 3;
    }

/*----------------------------- handle the last (probably partial) block */
/*
 * "k[2]&0xffffff" actually reads beyond the end of the string, but
 * then masks off the part it's not allowed to read.  Because the
 * string is aligned, the masked-off tail is in the same word as the
 * rest of the string.  Every machine with memory protection I've seen
 * does it on word boundaries, so is OK with this.  But VALGRIND will
 * still catch it and complain.  The masking trick does make the hash
 * noticably faster for short strings (like English words).
 */
#ifndef VALGRIND

    switch (length) {
      case 12:
        c += k[2];
        b += k[1];
        a += k[0];
        break;
      case 11:
        c += k[2] & 0xffffff;
        b += k[1];
        a += k[0];
        break;
      case 10:
        c += k[2] & 0xffff;
        b += k[1];
        a += k[0];
        break;
      case 9:
        c += k[2] & 0xff;
        b += k[1];
        a += k[0];
        break;
      case 8:
        b += k[1];
        a += k[0];
        break;
      case 7:
        b += k[1] & 0xffffff;
        a += k[0];
        break;
      case 6:
        b += k[1] & 0xffff;
        a += k[0];
        break;
      case 5:
        b += k[1] & 0xff;
        a += k[0];
        break;
      case 4:
        a += k[0];
        break;
      case 3:
        a += k[0] & 0xffffff;
        break;
      case 2:
        a += k[0] & 0xffff;
        break;
      case 1:
        a += k[0] & 0xff;
        break;
      case 0:
        return c; /* zero length strings require no mixing */
    }

#else /* make valgrind happy */

    const u_int8_t *k8;
    k8 = (const u_int8_t *)k;
    switch (length) {
      case 12:
        c += k[2];
        b += k[1];
        a += k[0];
        break;
      case 11:
        c += ((uint32_t)k8[10]) << 16; /* fall through */
      case 10:
        c += ((uint32_t)k8[9]) << 8; /* fall through */
      case 9:
        c += k8[8]; /* fall through */
      case 8:
        b += k[1];
        a += k[0];
        break;
      case 7:
        b += ((uint32_t)k8[6]) << 16; /* fall through */
      case 6:
        b += ((uint32_t)k8[5]) << 8; /* fall through */
      case 5:
        b += k8[4]; /* fall through */
      case 4:
        a += k[0];
        break;
      case 3:
        a += ((uint32_t)k8[2]) << 16; /* fall through */
      case 2:
        a += ((uint32_t)k8[1]) << 8; /* fall through */
      case 1:
        a += k8[0];
        break;
      case 0:
        return c;
    }

#endif /* !valgrind */

  } else if (HASH_LITTLE_ENDIAN && ((u.i & 0x1) == 0)) {
    const u_int16_t *k = (const u_int16_t *)buf; /* read 16-bit chunks */
    const u_int8_t *k8;

    /*--------------- all but last block: aligned reads and different mixing */
    while (length > 12) {
      a += k[0] + (((uint32_t)k[1]) << 16);
      b += k[2] + (((uint32_t)k[3]) << 16);
      c += k[4] + (((uint32_t)k[5]) << 16);
      mix(a, b, c);
      length -= 12;
      k += 6;
    }

    /*----------------------------- handle the last (probably partial) block */
    k8 = (const u_int8_t *)k;
    switch (length) {
      case 12:
        c += k[4] + (((uint32_t)k[5]) << 16);
        b += k[2] + (((uint32_t)k[3]) << 16);
        a += k[0] + (((uint32_t)k[1]) << 16);
        break;
      case 11:
        c += ((uint32_t)k8[10]) << 16; /* fall through */
      case 10:
        c += k[4];
        b += k[2] + (((uint32_t)k[3]) << 16);
        a += k[0] + (((uint32_t)k[1]) << 16);
        break;
      case 9:
        c += k8[8]; /* fall through */
      case 8:
        b += k[2] + (((uint32_t)k[3]) << 16);
        a += k[0] + (((uint32_t)k[1]) << 16);
        break;
      case 7:
        b += ((uint32_t)k8[6]) << 16; /* fall through */
      case 6:
        b += k[2];
        a += k[0] + (((uint32_t)k[1]) << 16);
        break;
      case 5:
        b += k8[4]; /* fall through */
      case 4:
        a += k[0] + (((uint32_t)k[1]) << 16);
        break;
      case 3:
        a += ((uint32_t)k8[2]) << 16; /* fall through */
      case 2:
        a += k[0];
        break;
      case 1:
        a += k8[0];
        break;
      case 0:
        return c; /* zero length requires no mixing */
    }

  } else { /* need to read the key one byte at a time */
    const u_int8_t *k = (const u_int8_t *)buf;

    /*--------------- all but the last block: affect some 32 bits of (a,b,c) */
    while (length > 12) {
      a += k[0];
      a += ((uint32_t)k[1]) << 8;
      a += ((uint32_t)k[2]) << 16;
      a += ((uint32_t)k[3]) << 24;
      b += k[4];
      b += ((uint32_t)k[5]) << 8;
      b += ((uint32_t)k[6]) << 16;
      b += ((uint32_t)k[7]) << 24;
      c += k[8];
      c += ((uint32_t)k[9]) << 8;
      c += ((uint32_t)k[10]) << 16;
      c += ((uint32_t)k[11]) << 24;
      mix(a, b, c);
      length -= 12;
      k += 12;
    }

    /*-------------------------------- last block: affect all 32 bits of (c) */
    switch (length) /* all the case statements fall through */
    {
      case 12:
        c += ((uint32_t)k[11]) << 24;
      case 11:
        c += ((uint32_t)k[10]) << 16;
      case 10:
        c += ((uint32_t)k[9]) << 8;
      case 9:
        c += k[8];
      case 8:
        b += ((uint32_t)k[7]) << 24;
      case 7:
        b += ((uint32_t)k[6]) << 16;
      case 6:
        b += ((uint32_t)k[5]) << 8;
      case 5:
        b += k[4];
      case 4:
        a += ((uint32_t)k[3]) << 24;
      case 3:
        a += ((uint32_t)k[2]) << 16;
      case 2:
        a += ((uint32_t)k[1]) << 8;
      case 1:
        a += k[0];
        break;
      case 0:
        return c;
    }
  }

  final(a, b, c);
  return c;
}

/*
 * hashlittle2: return 2 32-bit hash values
 *
 * This is identical to hashlittle(), except it returns two 32-bit hash
 * values instead of just one.  This is good enough for hash table
 * lookup with 2^^64 buckets, or if you want a second hash if you're not
 * happy with the first, or if you want a probably-unique 64-bit ID for
 * the key.  *pc is better mixed than *pb, so use *pc first.  If you want
 * a 64-bit value do something like "*pc + (((uint64_t)*pb)<<32)".
 */
void HashUtil::BobHash(const void *buf, size_t length, uint32_t *idx1,
                       uint32_t *idx2) {
  uint32_t a, b, c; /* internal state */
  union {
    const void *ptr;
    size_t i;
  } u; /* needed for Mac Powerbook G4 */

  /* Set up the internal state */
  a = b = c = 0xdeadbeef + ((uint32_t)length) + *idx1;
  c += *idx2;

  u.ptr = buf;
  if (HASH_LITTLE_ENDIAN && ((u.i & 0x3) == 0)) {
    const uint32_t *k = (const uint32_t *)buf; /* read 32-bit chunks */
#ifdef VALGRIND
    const uint8_t *k8;
#endif
    /*------ all but last block: aligned reads and affect 32 bits of (a,b,c) */
    while (length > 12) {
      a += k[0];
      b += k[1];
      c += k[2];
      mix(a, b, c);
      length -= 12;
      k += 3;
    }

/*----------------------------- handle the last (probably partial) block */
/*
 * "k[2]&0xffffff" actually reads beyond the end of the string, but
 * then masks off the part it's not allowed to read.  Because the
 * string is aligned, the masked-off tail is in the same word as the
 * rest of the string.  Every machine with memory protection I've seen
 * does it on word boundaries, so is OK with this.  But VALGRIND will
 * still catch it and complain.  The masking trick does make the hash
 * noticably faster for short strings (like English words).
 */
#ifndef VALGRIND

    switch (length) {
      case 12:
        c += k[2];
        b += k[1];
        a += k[0];
        break;
      case 11:
        c += k[2] & 0xffffff;
        b += k[1];
        a += k[0];
        break;
      case 10:
        c += k[2] & 0xffff;
        b += k[1];
        a += k[0];
        break;
      case 9:
        c += k[2] & 0xff;
        b += k[1];
        a += k[0];
        break;
      case 8:
        b += k[1];
        a += k[0];
        break;
      case 7:
        b += k[1] & 0xffffff;
        a += k[0];
        break;
      case 6:
        b += k[1] & 0xffff;
        a += k[0];
        break;
      case 5:
        b += k[1] & 0xff;
        a += k[0];
        break;
      case 4:
        a += k[0];
        break;
      case 3:
        a += k[0] & 0xffffff;
        break;
      case 2:
        a += k[0] & 0xffff;
        break;
      case 1:
        a += k[0] & 0xff;
        break;
      case 0:
        *idx1 = c;
        *idx2 = b;
        return; /* zero length strings require no mixing */
    }

#else /* make valgrind happy */

    k8 = (const uint8_t *)k;
    switch (length) {
      case 12:
        c += k[2];
        b += k[1];
        a += k[0];
        break;
      case 11:
        c += ((uint32_t)k8[10]) << 16; /* fall through */
      case 10:
        c += ((uint32_t)k8[9]) << 8; /* fall through */
      case 9:
        c += k8[8]; /* fall through */
      case 8:
        b += k[1];
        a += k[0];
        break;
      case 7:
        b += ((uint32_t)k8[6]) << 16; /* fall through */
      case 6:
        b += ((uint32_t)k8[5]) << 8; /* fall through */
      case 5:
        b += k8[4]; /* fall through */
      case 4:
        a += k[0];
        break;
      case 3:
        a += ((uint32_t)k8[2]) << 16; /* fall through */
      case 2:
        a += ((uint32_t)k8[1]) << 8; /* fall through */
      case 1:
        a += k8[0];
        break;
      case 0:
        *idx1 = c;
        *idx2 = b;
        return; /* zero length strings require no mixing */
    }

#endif /* !valgrind */

  } else if (HASH_LITTLE_ENDIAN && ((u.i & 0x1) == 0)) {
    const uint16_t *k = (const uint16_t *)buf; /* read 16-bit chunks */
    const uint8_t *k8;

    /*--------------- all but last block: aligned reads and different mixing */
    while (length > 12) {
      a += k[0] + (((uint32_t)k[1]) << 16);
      b += k[2] + (((uint32_t)k[3]) << 16);
      c += k[4] + (((uint32_t)k[5]) << 16);
      mix(a, b, c);
      length -= 12;
      k += 6;
    }

    /*----------------------------- handle the last (probably partial) block */
    k8 = (const uint8_t *)k;
    switch (length) {
      case 12:
        c += k[4] + (((uint32_t)k[5]) << 16);
        b += k[2] + (((uint32_t)k[3]) << 16);
        a += k[0] + (((uint32_t)k[1]) << 16);
        break;
      case 11:
        c += ((uint32_t)k8[10]) << 16; /* fall through */
      case 10:
        c += k[4];
        b += k[2] + (((uint32_t)k[3]) << 16);
        a += k[0] + (((uint32_t)k[1]) << 16);
        break;
      case 9:
        c += k8[8]; /* fall through */
      case 8:
        b += k[2] + (((uint32_t)k[3]) << 16);
        a += k[0] + (((uint32_t)k[1]) << 16);
        break;
      case 7:
        b += ((uint32_t)k8[6]) << 16; /* fall through */
      case 6:
        b += k[2];
        a += k[0] + (((uint32_t)k[1]) << 16);
        break;
      case 5:
        b += k8[4]; /* fall through */
      case 4:
        a += k[0] + (((uint32_t)k[1]) << 16);
        break;
      case 3:
        a += ((uint32_t)k8[2]) << 16; /* fall through */
      case 2:
        a += k[0];
        break;
      case 1:
        a += k8[0];
        break;
      case 0:
        *idx1 = c;
        *idx2 = b;
        return; /* zero length strings require no mixing */
    }

  } else { /* need to read the key one byte at a time */
    const uint8_t *k = (const uint8_t *)buf;

    /*--------------- all but the last block: affect some 32 bits of (a,b,c) */
    while (length > 12) {
      a += k[0];
      a += ((uint32_t)k[1]) << 8;
      a += ((uint32_t)k[2]) << 16;
      a += ((uint32_t)k[3]) << 24;
      b += k[4];
      b += ((uint32_t)k[5]) << 8;
      b += ((uint32_t)k[6]) << 16;
      b += ((uint32_t)k[7]) << 24;
      c += k[8];
      c += ((uint32_t)k[9]) << 8;
      c += ((uint32_t)k[10]) << 16;
      c += ((uint32_t)k[11]) << 24;
      mix(a, b, c);
      length -= 12;
      k += 12;
    }

    /*-------------------------------- last block: affect all 32 bits of (c) */
    switch (length) /* all the case statements fall through */
    {
      case 12:
        c += ((uint32_t)k[11]) << 24;
      case 11:
        c += ((uint32_t)k[10]) << 16;
      case 10:
        c += ((uint32_t)k[9]) << 8;
      case 9:
        c += k[8];
      case 8:
        b += ((uint32_t)k[7]) << 24;
      case 7:
        b += ((uint32_t)k[6]) << 16;
      case 6:
        b += ((uint32_t)k[5]) << 8;
      case 5:
        b += k[4];
      case 4:
        a += ((uint32_t)k[3]) << 24;
      case 3:
        a += ((uint32_t)k[2]) << 16;
      case 2:
        a += ((uint32_t)k[1]) << 8;
      case 1:
        a += k[0];
        break;
      case 0:
        *idx1 = c;
        *idx2 = b;
        return; /* zero length strings require no mixing */
    }
  }

  final(a, b, c);
  *idx1 = c;
  *idx2 = b;
}

void HashUtil::BobHash(const std::string &s, uint32_t *idx1, uint32_t *idx2) {
  return BobHash(s.data(), s.length(), idx1, idx2);
}

//-----------------------------------------------------------------------------
// MurmurHash2, by Austin Appleby
// Note - This code makes a few assumptions about how your machine behaves -
// 1. We can read a 4-byte value from any address without crashing
// 2. sizeof(int) == 4
// And it has a few limitations -
// 1. It will not work incrementally.
// 2. It will not produce the same results on little-endian and big-endian
//    machines.
// All code is released to the public domain. For business purposes,
// Murmurhash is under the MIT license.

uint32_t HashUtil::MurmurHash(const void *buf, size_t len, uint32_t seed) {
  // 'm' and 'r' are mixing constants generated offline.
  // They're not really 'magic', they just happen to work well.

  const unsigned int m = 0x5bd1e995;
  const int r = 24;

  // Initialize the hash to a 'random' value
  uint32_t h = seed ^ len;

  // Mix 4 bytes at a time into the hash
  const unsigned char *data = (const unsigned char *)buf;

  while (len >= 4) {
    unsigned int k = *(unsigned int *)data;

    k *= m;
    k ^= k >> r;
    k *= m;

    h *= m;
    h ^= k;

    data += 4;
    len -= 4;
  }

  // Handle the last few bytes of the input array
  switch (len) {
    case 3:
      h ^= data[2] << 16;
    case 2:
      h ^= data[1] << 8;
    case 1:
      h ^= data[0];
      h *= m;
  };

  // Do a few final mixes of the hash to ensure the last few
  // bytes are well-incorporated.
  h ^= h >> 13;
  h *= m;
  h ^= h >> 15;
  return h;
}

uint32_t HashUtil::MurmurHash(const std::string &s, uint32_t seed) {
  return MurmurHash(s.data(), s.length(), seed);
}

// SuperFastHash aka Hsieh Hash, License: GPL 2.0
uint32_t HashUtil::SuperFastHash(const void *buf, size_t len) {
  const char *data = (const char *)buf;
  uint32_t hash = len, tmp;
  int rem;

  if (len == 0 || data == NULL) return 0;

  rem = len & 3;
  len >>= 2;

  /* Main loop */
  for (; len > 0; len--) {
    hash += get16bits(data);
    tmp = (get16bits(data + 2) << 11) ^ hash;
    hash = (hash << 16) ^ tmp;
    data += 2 * sizeof(uint16_t);
    hash += hash >> 11;
  }

  /* Handle end cases */
  switch (rem) {
    case 3:
      hash += get16bits(data);
      hash ^= hash << 16;
      hash ^= data[sizeof(uint16_t)] << 18;
      hash += hash >> 11;
      break;
    case 2:
      hash += get16bits(data);
      hash ^= hash << 11;
      hash += hash >> 17;
      break;
    case 1:
      hash += *data;
      hash ^= hash << 10;
      hash += hash >> 1;
  }

  /* Force "avalanching" of final 127 bits */
  hash ^= hash << 3;
  hash += hash >> 5;
  hash ^= hash << 4;
  hash += hash >> 17;
  hash ^= hash << 25;
  hash += hash >> 6;

  return hash;
}

uint32_t HashUtil::SuperFastHash(const std::string &s) {
  return SuperFastHash(s.data(), s.length());
}

uint32_t HashUtil::NullHash(const void *buf, size_t length,
                            uint32_t shiftbytes) {
  // Ensure that enough bits exist in buffer
  if (length - shiftbytes < sizeof(uint32_t)) {
    return 0;
  }
  char *data = (char *)buf;
  return ((data[(length - shiftbytes - 4)] << 24) +
          (data[(length - shiftbytes - 3)] << 16) +
          (data[(length - shiftbytes - 2)] << 8) +
          (data[(length - shiftbytes - 1)]));
}

// ---- Self-contained MD5 / SHA1 (no OpenSSL) ------------------------------------
// Public domain implementations by Brad Conte (brad@bradconte.com)
// https://github.com/B-Con/crypto-algorithms

namespace {

// ---- MD5 ----------------------------------------------------------------
struct MD5_CTX2 {
    uint8_t  data[64];
    uint32_t datalen;
    uint64_t bitlen;
    uint32_t state[4];
};

#define MD5_ROTL(a,b) (((a) << (b)) | ((a) >> (32-(b))))
#define MD5_F(x,y,z)  (((x) & (y)) | (~(x) & (z)))
#define MD5_G(x,y,z)  (((x) & (z)) | ((y) & ~(z)))
#define MD5_H(x,y,z)  ((x) ^ (y) ^ (z))
#define MD5_I(x,y,z)  ((y) ^ ((x) | ~(z)))
#define MD5_FF(a,b,c,d,m,s,t) { (a) += MD5_F(b,c,d)+(m)+(t); (a) = (b)+MD5_ROTL(a,s); }
#define MD5_GG(a,b,c,d,m,s,t) { (a) += MD5_G(b,c,d)+(m)+(t); (a) = (b)+MD5_ROTL(a,s); }
#define MD5_HH(a,b,c,d,m,s,t) { (a) += MD5_H(b,c,d)+(m)+(t); (a) = (b)+MD5_ROTL(a,s); }
#define MD5_II(a,b,c,d,m,s,t) { (a) += MD5_I(b,c,d)+(m)+(t); (a) = (b)+MD5_ROTL(a,s); }

static void md5_transform(MD5_CTX2 *ctx, const uint8_t *data) {
    uint32_t a, b, c, d, m[16];
    for (int i = 0, j = 0; i < 16; ++i, j += 4)
        m[i] = (uint32_t)data[j] | ((uint32_t)data[j+1]<<8) | ((uint32_t)data[j+2]<<16) | ((uint32_t)data[j+3]<<24);
    a=ctx->state[0]; b=ctx->state[1]; c=ctx->state[2]; d=ctx->state[3];
    MD5_FF(a,b,c,d,m[ 0], 7,0xd76aa478); MD5_FF(d,a,b,c,m[ 1],12,0xe8c7b756);
    MD5_FF(c,d,a,b,m[ 2],17,0x242070db); MD5_FF(b,c,d,a,m[ 3],22,0xc1bdceee);
    MD5_FF(a,b,c,d,m[ 4], 7,0xf57c0faf); MD5_FF(d,a,b,c,m[ 5],12,0x4787c62a);
    MD5_FF(c,d,a,b,m[ 6],17,0xa8304613); MD5_FF(b,c,d,a,m[ 7],22,0xfd469501);
    MD5_FF(a,b,c,d,m[ 8], 7,0x698098d8); MD5_FF(d,a,b,c,m[ 9],12,0x8b44f7af);
    MD5_FF(c,d,a,b,m[10],17,0xffff5bb1); MD5_FF(b,c,d,a,m[11],22,0x895cd7be);
    MD5_FF(a,b,c,d,m[12], 7,0x6b901122); MD5_FF(d,a,b,c,m[13],12,0xfd987193);
    MD5_FF(c,d,a,b,m[14],17,0xa679438e); MD5_FF(b,c,d,a,m[15],22,0x49b40821);
    MD5_GG(a,b,c,d,m[ 1], 5,0xf61e2562); MD5_GG(d,a,b,c,m[ 6], 9,0xc040b340);
    MD5_GG(c,d,a,b,m[11],14,0x265e5a51); MD5_GG(b,c,d,a,m[ 0],20,0xe9b6c7aa);
    MD5_GG(a,b,c,d,m[ 5], 5,0xd62f105d); MD5_GG(d,a,b,c,m[10], 9,0x02441453);
    MD5_GG(c,d,a,b,m[15],14,0xd8a1e681); MD5_GG(b,c,d,a,m[ 4],20,0xe7d3fbc8);
    MD5_GG(a,b,c,d,m[ 9], 5,0x21e1cde6); MD5_GG(d,a,b,c,m[14], 9,0xc33707d6);
    MD5_GG(c,d,a,b,m[ 3],14,0xf4d50d87); MD5_GG(b,c,d,a,m[ 8],20,0x455a14ed);
    MD5_GG(a,b,c,d,m[13], 5,0xa9e3e905); MD5_GG(d,a,b,c,m[ 2], 9,0xfcefa3f8);
    MD5_GG(c,d,a,b,m[ 7],14,0x676f02d9); MD5_GG(b,c,d,a,m[12],20,0x8d2a4c8a);
    MD5_HH(a,b,c,d,m[ 5], 4,0xfffa3942); MD5_HH(d,a,b,c,m[ 8],11,0x8771f681);
    MD5_HH(c,d,a,b,m[11],16,0x6d9d6122); MD5_HH(b,c,d,a,m[14],23,0xfde5380c);
    MD5_HH(a,b,c,d,m[ 1], 4,0xa4beea44); MD5_HH(d,a,b,c,m[ 4],11,0x4bdecfa9);
    MD5_HH(c,d,a,b,m[ 7],16,0xf6bb4b60); MD5_HH(b,c,d,a,m[10],23,0xbebfbc70);
    MD5_HH(a,b,c,d,m[13], 4,0x289b7ec6); MD5_HH(d,a,b,c,m[ 0],11,0xeaa127fa);
    MD5_HH(c,d,a,b,m[ 3],16,0xd4ef3085); MD5_HH(b,c,d,a,m[ 6],23,0x04881d05);
    MD5_HH(a,b,c,d,m[ 9], 4,0xd9d4d039); MD5_HH(d,a,b,c,m[12],11,0xe6db99e5);
    MD5_HH(c,d,a,b,m[15],16,0x1fa27cf8); MD5_HH(b,c,d,a,m[ 2],23,0xc4ac5665);
    MD5_II(a,b,c,d,m[ 0], 6,0xf4292244); MD5_II(d,a,b,c,m[ 7],10,0x432aff97);
    MD5_II(c,d,a,b,m[14],15,0xab9423a7); MD5_II(b,c,d,a,m[ 5],21,0xfc93a039);
    MD5_II(a,b,c,d,m[12], 6,0x655b59c3); MD5_II(d,a,b,c,m[ 3],10,0x8f0ccc92);
    MD5_II(c,d,a,b,m[10],15,0xffeff47d); MD5_II(b,c,d,a,m[ 1],21,0x85845dd1);
    MD5_II(a,b,c,d,m[ 8], 6,0x6fa87e4f); MD5_II(d,a,b,c,m[15],10,0xfe2ce6e0);
    MD5_II(c,d,a,b,m[ 6],15,0xa3014314); MD5_II(b,c,d,a,m[13],21,0x4e0811a1);
    MD5_II(a,b,c,d,m[ 4], 6,0xf7537e82); MD5_II(d,a,b,c,m[11],10,0xbd3af235);
    MD5_II(c,d,a,b,m[ 2],15,0x2ad7d2bb); MD5_II(b,c,d,a,m[ 9],21,0xeb86d391);
    ctx->state[0]+=a; ctx->state[1]+=b; ctx->state[2]+=c; ctx->state[3]+=d;
}
static void md5_init(MD5_CTX2 *ctx) {
    ctx->datalen=0; ctx->bitlen=0;
    ctx->state[0]=0x67452301; ctx->state[1]=0xefcdab89;
    ctx->state[2]=0x98badcfe; ctx->state[3]=0x10325476;
}
static void md5_update(MD5_CTX2 *ctx, const uint8_t *data, size_t len) {
    for (size_t i=0; i<len; ++i) {
        ctx->data[ctx->datalen++] = data[i];
        if (ctx->datalen==64) { md5_transform(ctx,ctx->data); ctx->bitlen+=512; ctx->datalen=0; }
    }
}
static void md5_final(MD5_CTX2 *ctx, uint8_t hash[16]) {
    uint32_t i=ctx->datalen;
    if (i<56) { ctx->data[i++]=0x80; while(i<56) ctx->data[i++]=0; }
    else { ctx->data[i++]=0x80; while(i<64) ctx->data[i++]=0; md5_transform(ctx,ctx->data); memset(ctx->data,0,56); }
    ctx->bitlen+=ctx->datalen*8;
    for(int b=0;b<8;b++) ctx->data[56+b]=(uint8_t)(ctx->bitlen>>(b*8));
    md5_transform(ctx,ctx->data);
    for(i=0;i<4;++i) { hash[i]=(uint8_t)(ctx->state[0]>>(i*8)); hash[i+4]=(uint8_t)(ctx->state[1]>>(i*8)); hash[i+8]=(uint8_t)(ctx->state[2]>>(i*8)); hash[i+12]=(uint8_t)(ctx->state[3]>>(i*8)); }
}

// ---- SHA1 ---------------------------------------------------------------
struct SHA1_CTX2 {
    uint8_t  data[64];
    uint32_t datalen;
    uint64_t bitlen;
    uint32_t state[5];
};
#define SHA1_ROTL(a,b) (((a)<<(b))|((a)>>(32-(b))))
static void sha1_transform(SHA1_CTX2 *ctx, const uint8_t *data) {
    uint32_t a,b,c,d,e,t,m[80];
    for(int i=0,j=0;i<16;++i,j+=4)
        m[i]=((uint32_t)data[j]<<24)|((uint32_t)data[j+1]<<16)|((uint32_t)data[j+2]<<8)|data[j+3];
    for(int i=16;i<80;++i) m[i]=SHA1_ROTL(m[i-3]^m[i-8]^m[i-14]^m[i-16],1);
    a=ctx->state[0];b=ctx->state[1];c=ctx->state[2];d=ctx->state[3];e=ctx->state[4];
    for(int i=0;i<80;++i) {
        if(i<20)      t=SHA1_ROTL(a,5)+((b&c)|(~b&d))+e+0x5a827999+m[i];
        else if(i<40) t=SHA1_ROTL(a,5)+(b^c^d)+e+0x6ed9eba1+m[i];
        else if(i<60) t=SHA1_ROTL(a,5)+((b&c)|(b&d)|(c&d))+e+0x8f1bbcdc+m[i];
        else          t=SHA1_ROTL(a,5)+(b^c^d)+e+0xca62c1d6+m[i];
        e=d;d=c;c=SHA1_ROTL(b,30);b=a;a=t;
    }
    ctx->state[0]+=a;ctx->state[1]+=b;ctx->state[2]+=c;ctx->state[3]+=d;ctx->state[4]+=e;
}
static void sha1_init(SHA1_CTX2 *ctx) {
    ctx->datalen=0; ctx->bitlen=0;
    ctx->state[0]=0x67452301;ctx->state[1]=0xefcdab89;ctx->state[2]=0x98badcfe;
    ctx->state[3]=0x10325476;ctx->state[4]=0xc3d2e1f0;
}
static void sha1_update(SHA1_CTX2 *ctx, const uint8_t *data, size_t len) {
    for(size_t i=0;i<len;++i) {
        ctx->data[ctx->datalen++]=data[i];
        if(ctx->datalen==64) { sha1_transform(ctx,ctx->data); ctx->bitlen+=512; ctx->datalen=0; }
    }
}
static void sha1_final(SHA1_CTX2 *ctx, uint8_t hash[20]) {
    uint32_t i=ctx->datalen;
    if(i<56) { ctx->data[i++]=0x80; while(i<56) ctx->data[i++]=0; }
    else { ctx->data[i++]=0x80; while(i<64) ctx->data[i++]=0; sha1_transform(ctx,ctx->data); memset(ctx->data,0,56); }
    ctx->bitlen+=ctx->datalen*8;
    for(int b=7;b>=0;b--) ctx->data[56+(7-b)]=(uint8_t)(ctx->bitlen>>(b*8));
    sha1_transform(ctx,ctx->data);
    for(i=0;i<4;++i) { hash[i]=(uint8_t)(ctx->state[0]>>(24-i*8)); hash[i+4]=(uint8_t)(ctx->state[1]>>(24-i*8)); hash[i+8]=(uint8_t)(ctx->state[2]>>(24-i*8)); hash[i+12]=(uint8_t)(ctx->state[3]>>(24-i*8)); hash[i+16]=(uint8_t)(ctx->state[4]>>(24-i*8)); }
}

} // anonymous namespace

std::string HashUtil::MD5Hash(const char *inbuf, size_t in_length) {
    MD5_CTX2 ctx; uint8_t digest[16];
    md5_init(&ctx);
    md5_update(&ctx, reinterpret_cast<const uint8_t *>(inbuf), in_length);
    md5_final(&ctx, digest);
    return std::string(reinterpret_cast<char *>(digest), 16);
}

std::string HashUtil::SHA1Hash(const char *inbuf, size_t in_length) {
    SHA1_CTX2 ctx; uint8_t digest[20];
    sha1_init(&ctx);
    sha1_update(&ctx, reinterpret_cast<const uint8_t *>(inbuf), in_length);
    sha1_final(&ctx, digest);
    return std::string(reinterpret_cast<char *>(digest), 20);
}

}  // namespace cuckoofilter
