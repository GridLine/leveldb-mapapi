/**
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.gridline.leveldb.bindings.utils;

/**
 * Simple Byte array conversions.<br />
 * Subset of the HBase bytes util: {@link org.apache.hadoop.hbase.util.Bytes}
 * @author <a href="mailto:job@gridline.nl">Job</a>
 */
public class Bytes
{

	public static final int SIZE_OF_INT = Integer.SIZE / 8;
	public static final int SIZE_OF_LONG = Long.SIZE / 8;

	public static int toInt(byte[] bytes)
	{
		int n = 0;
		for (int i = 0; i < SIZE_OF_INT; i++)
		{
			n <<= 8;
			n ^= bytes[i] & 0xFF;
		}
		return n;
	}

	public static long toLong(byte[] bytes)
	{
		long l = 0;
		for (int i = 0; i < SIZE_OF_LONG; i++)
		{
			l <<= 8;
			l ^= bytes[i] & 0xFF;
		}
		return l;
	}

	public static double toDouble(final byte[] bytes)
	{
		return Double.longBitsToDouble(toLong(bytes));
	}

	public static float toFloat(byte[] bytes)
	{
		return Float.intBitsToFloat(toInt(bytes));
	}

	public static byte[] toBytes(int val)
	{
		byte[] b = new byte[4];
		for (int i = 3; i > 0; i--)
		{
			b[i] = (byte) val;
			val >>>= 8;
		}
		b[0] = (byte) val;
		return b;
	}

	public static byte[] toBytes(long val)
	{
		byte[] b = new byte[8];
		for (int i = 7; i > 0; i--)
		{
			b[i] = (byte) val;
			val >>>= 8;
		}
		b[0] = (byte) val;
		return b;
	}

	public static byte[] toBytes(float f)
	{
		return toBytes(Float.floatToRawIntBits(f));
	}

	public static byte[] toBytes(final double d)
	{
		return Bytes.toBytes(Double.doubleToRawLongBits(d));
	}

}
