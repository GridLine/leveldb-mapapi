// Copyright 2013 GridLine
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nl.gridline.leveldb.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Non synchronized, no null checking version of a {@link ByteArrayOutputStream} - that should be a bit faster.
 * @author <a href="mailto:job@gridline.nl">Job</a>
 */
public class FastOutputStream extends OutputStream
{

	protected byte[] buf = null;

	protected int size = 0;

	public FastOutputStream()
	{
		this(1024);
	}

	public FastOutputStream(int size)
	{
		this.size = 0;
		buf = new byte[size];
	}

	private void ensure(int sz)
	{
		if (sz > buf.length)
		{
			byte[] current = buf;
			buf = new byte[Math.max(sz, 2 * buf.length)];
			System.arraycopy(current, 0, buf, 0, current.length);
			current = null;
		}
	}

	public int getSize()
	{
		return size;
	}

	public byte[] getBuf()
	{
		return buf;
	}

	public byte[] toArray()
	{
		if (size == buf.length)
		{
			return buf;
		}
		final byte[] result = new byte[size];
		System.arraycopy(buf, 0, result, 0, Math.min(buf.length, size));
		return result;
	}

	@Override
	public void write(int b) throws IOException
	{
		ensure(size + 1);
		buf[size++] = (byte) b;
	}

	@Override
	public void write(byte[] b) throws IOException
	{
		ensure(size + b.length);
		System.arraycopy(b, 0, buf, size, b.length);
		size += b.length;
	}

	@Override
	public void write(byte[] b, int offset, int length) throws IOException
	{
		ensure(size + length);
		System.arraycopy(b, offset, buf, size, length);
		size += length;
	}

}
