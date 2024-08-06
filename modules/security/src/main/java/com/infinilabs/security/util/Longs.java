/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.infinilabs.security.util;

/**
 * Utility methods and constants for longs.
 */
public class Longs
{
    public static final int BYTES = 8;
    public static final int SIZE = Long.SIZE;

    public static int numberOfLeadingZeros(long i)
    {
        return Long.numberOfLeadingZeros(i);
    }

    public static int numberOfTrailingZeros(long i)
    {
        return Long.numberOfTrailingZeros(i);
    }

    public static long reverse(long i)
    {
        return Long.reverse(i);
    }

    public static long reverseBytes(long i)
    {
        return Long.reverseBytes(i);
    }

    public static long rotateLeft(long i, int distance)
    {
        return Long.rotateLeft(i, distance);
    }

    public static long rotateRight(long i, int distance)
    {
        return Long.rotateRight(i, distance);
    }

    public static Long valueOf(long value)
    {
        return Long.valueOf(value);
    }
}
