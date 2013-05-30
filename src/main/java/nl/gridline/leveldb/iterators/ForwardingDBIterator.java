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

package nl.gridline.leveldb.iterators;

import java.io.IOException;
import java.util.Map.Entry;

import org.iq80.leveldb.DBIterator;

/**
 * A DBIterator which forwards method calls to another DBIterator. A subclass can override one or more methods to
 * change only part of the behavior of DBIterator.
 * @author <a href="mailto:niels@gridline.nl">Niels Slot</a>
 */
public abstract class ForwardingDBIterator implements DBIterator
{

	protected abstract DBIterator delegate();

	@Override
	public boolean hasNext()
	{
		return delegate().hasNext();
	}

	@Override
	public Entry<byte[], byte[]> next()
	{
		return delegate().next();
	}

	@Override
	public void remove()
	{
		delegate().remove();
	}

	@Override
	public void close() throws IOException
	{
		delegate().close();
	}

	@Override
	public void seek(byte[] key)
	{
		delegate().seek(key);
	}

	@Override
	public void seekToFirst()
	{
		delegate().seekToFirst();
	}

	@Override
	public Entry<byte[], byte[]> peekNext()
	{
		return delegate().peekNext();
	}

	@Override
	public boolean hasPrev()
	{
		return delegate().hasPrev();
	}

	@Override
	public Entry<byte[], byte[]> prev()
	{
		return delegate().prev();
	}

	@Override
	public Entry<byte[], byte[]> peekPrev()
	{
		return delegate().peekPrev();
	}

	@Override
	public void seekToLast()
	{
		delegate().seekToLast();
	}
}
