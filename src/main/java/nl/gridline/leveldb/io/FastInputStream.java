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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Non synchronized, no null checking version of a {@link ByteArrayInputStream} - that should be a bit faster.
 * @author <a href="mailto:job@gridline.nl">Job</a>
 */
public class FastInputStream extends InputStream
{

	protected byte[] buffer = null;
	protected int count = 0;
	protected int pos = 0;

	public FastInputStream(byte[] buffer)
	{
		this.buffer = buffer;
		count = buffer.length;
	}

	@Override
	public int read() throws IOException
	{
		return (pos < count) ? (buffer[pos++] & 0xff) : -1;
	}

	@Override
	public int available() throws IOException
	{
		return count - pos;
	}

	@Override
	public int read(byte[] b) throws IOException
	{
		return read(b, 0, pos);
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException
	{
		if (pos >= count)
		{
			return -1;
		}

		if ((pos + len) > count)
		{
			len = (count - pos);
		}
		System.arraycopy(buffer, pos, b, off, len);
		pos += len;
		return len;
	}

	@Override
	public long skip(long n) throws IOException
	{
		if ((pos + n) > count)
		{
			n = count - pos;
		}
		if (n < 0)
		{
			return 0;
		}
		pos += n;
		return n;
	}

}
