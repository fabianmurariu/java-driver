/*
 *      Copyright (C) 2012-2014 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import com.datastax.driver.core.utils.Bytes;

/**
 * A token on the Cassandra ring.
 */
public abstract class Token implements Comparable<Token> {

    /**
     * Returns a serialized representation of this token, suitable to bind in a statement.
     *
     * @return the serialized representation.
     */
    public abstract ByteBuffer serialize();

    /**
     * Returns whether this token is the minimum token on the ring.
     *
     * @return whether this token is the minimum token on the ring
     */
    public abstract boolean isMinToken();

    static Token.Factory getFactory(String partitionerName) {
        if (partitionerName.endsWith("Murmur3Partitioner"))
            return M3PToken.FACTORY;
        else if (partitionerName.endsWith("RandomPartitioner"))
            return RPToken.FACTORY;
        else if (partitionerName.endsWith("OrderedPartitioner"))
            return OPPToken.FACTORY;
        else
            return null;
    }

    static abstract class Factory {
        abstract Token fromString(String tokenStr);
        abstract Token hash(ByteBuffer partitionKey);
        abstract List<Token> split(Token startToken, Token endToken, int numberOfSplits);

        // Base implementation for split
        protected List<Token> split(BigInteger start, BigInteger range,
                                    BigInteger ringEnd, BigInteger ringLength,
                                    int numberOfSplits) {
            BigInteger[] tmp = range.divideAndRemainder(BigInteger.valueOf(numberOfSplits));
            BigInteger divider = tmp[0];
            int remainder = tmp[1].intValue();

            List<Token> results = Lists.newArrayListWithExpectedSize(numberOfSplits - 1);
            BigInteger current = start;
            BigInteger dividerPlusOne = (remainder == 0) ? null // won't be used
                : divider.add(BigInteger.ONE);

            for (int i = 1; i < numberOfSplits; i++) {
                current = current.add(remainder-- > 0 ? dividerPlusOne : divider);
                if (ringEnd != null && current.compareTo(ringEnd) > 0)
                    current = current.subtract(ringLength);
                results.add(newToken(current));
            }
            return results;
        }
        protected abstract Token newToken(BigInteger value);
    }

    // Murmur3Partitioner tokens
    static class M3PToken extends Token {
        private final long value;

        private static final BigInteger RING_END = BigInteger.valueOf(Long.MAX_VALUE);
        private static final BigInteger RING_LENGTH = RING_END.add(BigInteger.ONE).subtract(BigInteger.valueOf(Long.MIN_VALUE));

        public static final Factory FACTORY = new Factory() {

            private long getblock(ByteBuffer key, int offset, int index) {
                int i_8 = index << 3;
                int blockOffset = offset + i_8;
                return ((long) key.get(blockOffset + 0) & 0xff) + (((long) key.get(blockOffset + 1) & 0xff) << 8) +
                       (((long) key.get(blockOffset + 2) & 0xff) << 16) + (((long) key.get(blockOffset + 3) & 0xff) << 24) +
                       (((long) key.get(blockOffset + 4) & 0xff) << 32) + (((long) key.get(blockOffset + 5) & 0xff) << 40) +
                       (((long) key.get(blockOffset + 6) & 0xff) << 48) + (((long) key.get(blockOffset + 7) & 0xff) << 56);
            }

            private long rotl64(long v, int n) {
                return ((v << n) | (v >>> (64 - n)));
            }

            private long fmix(long k) {
                k ^= k >>> 33;
                k *= 0xff51afd7ed558ccdL;
                k ^= k >>> 33;
                k *= 0xc4ceb9fe1a85ec53L;
                k ^= k >>> 33;
                return k;
            }

            // This is an adapted version of the MurmurHash.hash3_x64_128 from Cassandra used
            // for M3P. Compared to that methods, there's a few inlining of arguments and we
            // only return the first 64-bits of the result since that's all M3P uses.
            @SuppressWarnings("fallthrough")
            private long murmur(ByteBuffer data) {
                int offset = data.position();
                int length = data.remaining();

                int nblocks = length >> 4; // Process as 128-bit blocks.

                long h1 = 0;
                long h2 = 0;

                long c1 = 0x87c37b91114253d5L;
                long c2 = 0x4cf5ad432745937fL;

                //----------
                // body

                for(int i = 0; i < nblocks; i++) {
                    long k1 = getblock(data, offset, i*2+0);
                    long k2 = getblock(data, offset, i*2+1);

                    k1 *= c1; k1 = rotl64(k1,31); k1 *= c2; h1 ^= k1;
                    h1 = rotl64(h1,27); h1 += h2; h1 = h1*5+0x52dce729;
                    k2 *= c2; k2  = rotl64(k2,33); k2 *= c1; h2 ^= k2;
                    h2 = rotl64(h2,31); h2 += h1; h2 = h2*5+0x38495ab5;
                }

                //----------
                // tail

                // Advance offset to the unprocessed tail of the data.
                offset += nblocks * 16;

                long k1 = 0;
                long k2 = 0;

                switch(length & 15) {
                    case 15: k2 ^= ((long) data.get(offset+14)) << 48;
                    case 14: k2 ^= ((long) data.get(offset+13)) << 40;
                    case 13: k2 ^= ((long) data.get(offset+12)) << 32;
                    case 12: k2 ^= ((long) data.get(offset+11)) << 24;
                    case 11: k2 ^= ((long) data.get(offset+10)) << 16;
                    case 10: k2 ^= ((long) data.get(offset+9)) << 8;
                    case  9: k2 ^= ((long) data.get(offset+8)) << 0;
                             k2 *= c2; k2  = rotl64(k2,33); k2 *= c1; h2 ^= k2;

                    case  8: k1 ^= ((long) data.get(offset+7)) << 56;
                    case  7: k1 ^= ((long) data.get(offset+6)) << 48;
                    case  6: k1 ^= ((long) data.get(offset+5)) << 40;
                    case  5: k1 ^= ((long) data.get(offset+4)) << 32;
                    case  4: k1 ^= ((long) data.get(offset+3)) << 24;
                    case  3: k1 ^= ((long) data.get(offset+2)) << 16;
                    case  2: k1 ^= ((long) data.get(offset+1)) << 8;
                    case  1: k1 ^= ((long) data.get(offset));
                             k1 *= c1; k1  = rotl64(k1,31); k1 *= c2; h1 ^= k1;
                };

                //----------
                // finalization

                h1 ^= length; h2 ^= length;

                h1 += h2;
                h2 += h1;

                h1 = fmix(h1);
                h2 = fmix(h2);

                h1 += h2;
                h2 += h1;

                return h1;
            }

            @Override
            public M3PToken fromString(String tokenStr) {
                return new M3PToken(Long.parseLong(tokenStr));
            }

            @Override
            public M3PToken hash(ByteBuffer partitionKey) {
                long v = murmur(partitionKey);
                return new M3PToken(v == Long.MIN_VALUE ? Long.MAX_VALUE : v);
            }

            @Override
            List<Token> split(Token startToken, Token endToken, int numberOfSplits) {
                if (startToken.compareTo(endToken) == 0)
                    throw new IllegalArgumentException("Cannot split range with equal bounds");

                BigInteger start = BigInteger.valueOf(((M3PToken)startToken).value);
                BigInteger end = BigInteger.valueOf(((M3PToken)endToken).value);

                BigInteger range = end.subtract(start);
                if (range.compareTo(BigInteger.ZERO) < 0)
                    range = range.add(RING_LENGTH);

                return super.split(start, range,
                    RING_END, RING_LENGTH,
                    numberOfSplits);
            }

            @Override
            protected Token newToken(BigInteger value) {
                return new M3PToken(value.longValue());
            }
        };

        private M3PToken(long value) {
            this.value = value;
        }

        @Override
        public int compareTo(Token other) {
            assert other instanceof M3PToken;
            long otherValue = ((M3PToken)other).value;
            return value < otherValue ? -1 : (value == otherValue) ? 0 : 1;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null || this.getClass() != obj.getClass())
                return false;

            return value == ((M3PToken)obj).value;
        }

        @Override
        public int hashCode() {
            return (int)(value^(value>>>32));
        }

        @Override
        public ByteBuffer serialize() {
            return DataType.bigint().serialize(value);
        }

        @Override
        public boolean isMinToken() {
            return value == Long.MIN_VALUE;
        }

        @Override
        public String toString() {
            return "M3PToken(" + value + ")";
        }
    }

    // OPPartitioner tokens
    static class OPPToken extends Token {
        private static final BigInteger TWO = BigInteger.valueOf(2);

        private final ByteBuffer value;

        public static final Factory FACTORY = new Factory() {
            @Override
            public OPPToken fromString(String tokenStr) {
                return new OPPToken(TypeCodec.StringCodec.utf8Instance.serialize(tokenStr));
            }

            @Override
            public OPPToken hash(ByteBuffer partitionKey) {
                return new OPPToken(partitionKey);
            }

            @Override
            public List<Token> split(Token startToken, Token endToken, int numberOfSplits) {
                int tokenOrder = startToken.compareTo(endToken);
                if (tokenOrder == 0)
                    throw new IllegalArgumentException("Cannot split range with equal bounds");

                OPPToken oppStartToken = (OPPToken)startToken;
                OPPToken oppEndToken = (OPPToken)endToken;

                int significantBytes;
                BigInteger start, end, range, ringEnd, ringLength;
                BigInteger bigNumberOfSplits = BigInteger.valueOf(numberOfSplits);
                if (tokenOrder < 0) {
                    // Since tokens are compared lexicographically, convert to integers using the largest length
                    // (ex: given 0x0A and 0x0BCD, switch to 0x0A00 and 0x0BCD)
                    significantBytes = Math.max(oppStartToken.value.capacity(), oppEndToken.value.capacity());

                    // If the number of splits does not fit in the difference between the two integers, use more bytes
                    // (ex: cannot fit 4 splits between 0x01 and 0x03, so switch to 0x0100 and 0x0300)
                    // At most 4 additional bytes will be needed, since numberOfSplits is an integer.
                    int addedBytes = 0;
                    while (true) {
                        start = toBigInteger(oppStartToken.value, significantBytes);
                        end = toBigInteger(oppEndToken.value, significantBytes);
                        range = end.subtract(start);
                        if (addedBytes == 4 || range.compareTo(bigNumberOfSplits) >= 0)
                            break;
                        significantBytes += 1;
                        addedBytes += 1;
                    }
                    ringEnd = ringLength = null; // won't be used
                } else {
                    // Same logic except that we wrap around the ring
                    significantBytes = Math.max(oppStartToken.value.capacity(), oppEndToken.value.capacity());
                    int addedBytes = 0;
                    while (true) {
                        start = toBigInteger(oppStartToken.value, significantBytes);
                        end = toBigInteger(oppEndToken.value, significantBytes);
                        ringEnd = TWO.pow(significantBytes * 8);
                        ringLength = ringEnd.add(BigInteger.ONE);
                        range = end.subtract(start).add(ringLength);
                        if (addedBytes == 4 || range.compareTo(bigNumberOfSplits) >= 0)
                            break;
                        significantBytes += 1;
                        addedBytes += 1;
                    }
                }

                return super.split(start, range,
                    ringEnd, ringLength,
                    numberOfSplits);
            }

            @Override
            protected Token newToken(BigInteger value) {
                return new OPPToken(ByteBuffer.wrap(value.toByteArray()));
            }

            private BigInteger toBigInteger(ByteBuffer bb, int significantBytes) {
                byte[] bytes = Bytes.getArray(bb);
                byte[] target;
                if (significantBytes != bytes.length) {
                    target = new byte[significantBytes];
                    System.arraycopy(bytes, 0, target, 0, bytes.length);
                } else
                    target = bytes;
                return new BigInteger(1, target);
            }
        };

        @VisibleForTesting
        OPPToken(ByteBuffer value) {
            this.value = value;
        }

        @Override
        public int compareTo(Token other) {
            assert other instanceof OPPToken;
            return value.compareTo(((OPPToken)other).value);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null || this.getClass() != obj.getClass())
                return false;

            return value.equals(((OPPToken)obj).value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }

        @Override
        public ByteBuffer serialize() {
            return value;
        }

        @Override
        public boolean isMinToken() {
            return value.capacity() == 0;
        }

        @Override
        public String toString() {
            return "OPPToken(" + Bytes.toHexString(value) + ")";
        }
    }

    // RandomPartitioner tokens
    static class RPToken extends Token {

        private final BigInteger value;

        private static final BigInteger MIN_VALUE = BigInteger.ONE.negate();
        private static final BigInteger MAX_VALUE = BigInteger.valueOf(2).pow(127);
        private static final BigInteger RING_LENGTH = MAX_VALUE.add(BigInteger.ONE);

        public static final Factory FACTORY = new Factory() {

            private BigInteger md5(ByteBuffer data) {
                try {
                    MessageDigest digest = MessageDigest.getInstance("MD5");
                    digest.update(data.duplicate());
                    return new BigInteger(digest.digest()).abs();
                } catch (NoSuchAlgorithmException e) {
                    throw new RuntimeException("MD5 doesn't seem to be available on this JVM", e);
                }
            }

            @Override
            public RPToken fromString(String tokenStr) {
                return new RPToken(new BigInteger(tokenStr));
            }

            @Override
            public RPToken hash(ByteBuffer partitionKey) {
                return new RPToken(md5(partitionKey));
            }

            @Override
            List<Token> split(Token startToken, Token endToken, int numberOfSplits) {
                if (startToken.compareTo(endToken) == 0)
                    throw new IllegalArgumentException("Cannot split range with equal bounds");

                BigInteger start = ((RPToken)startToken).value;
                BigInteger end = ((RPToken)endToken).value;

                BigInteger range = end.subtract(start);
                if (range.compareTo(BigInteger.ZERO) < 0)
                    range = range.add(RING_LENGTH);

                return super.split(start, range,
                    MAX_VALUE, RING_LENGTH,
                    numberOfSplits);
            }

            @Override
            protected Token newToken(BigInteger value) {
                return new RPToken(value);
            }
        };

        private RPToken(BigInteger value) {
            this.value = value;
        }

        @Override
        public int compareTo(Token other) {
            assert other instanceof RPToken;
            return value.compareTo(((RPToken)other).value);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null || this.getClass() != obj.getClass())
                return false;

            return value.equals(((RPToken)obj).value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }

        @Override
        public ByteBuffer serialize() {
            return DataType.varint().serialize(value);
        }

        @Override
        public boolean isMinToken() {
            return value.equals(MIN_VALUE);
        }

        @Override
        public String toString() {
            return "RPToken(" + value + ")";
        }
    }
}
