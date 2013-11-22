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

package nl.gridline.leveldb;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SortedMap;

import nl.gridline.leveldb.comparators.SimpleDBComparator;
import nl.gridline.leveldb.iterators.ForwardingDBIterator;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBComparator;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.WriteBatch;

/**
 * A SortedMap implementation on top of LevelDB.
 *
 * Note that the following methods (besides the usual suspects) are O(n):
 *
 * <ul>
 *     <li>{@link #clear()}</li>
 *     <li>{@link #lastKey()}</li>
 *     <li>{@link #size()} (use {@link #isEmpty()} rather than <i>size() == 0)</i></li>
 *     <li>All methods of the object returned by {@link #entrySet()} (except obtaining an iterator).</li>
 *     <li>All methods of the object returned by {@link #keySet()} (except obtaining an iterator).</li>
 *     <li>All methods of the object returned by {@link #values()} (except obtaining an iterator).</li>
 * </ul>
 *
 * @author <a href="mailto:niels@gridline.nl">Niels Slot</a>
 */
public class LevelDBStoredSortedMap<K, V> extends LevelDBStoredMap<K, V> implements StoredSortedMap<K, V>
{

	private final byte[] start;
	private final byte[] end;
	private final Comparator<? super K> comparator;
	private DBComparator dbcomparator;

	public LevelDBStoredSortedMap(DB db, DBComparator dbcomparator, EntryBinding<K> keyBinding,
			EntryBinding<V> valueBinding)
	{
		this(db, dbcomparator, keyBinding, valueBinding, null, null, null);
	}

	public LevelDBStoredSortedMap(DB db, DBComparator dbcomparator, EntryBinding<K> keyBinding,
			EntryBinding<V> valueBinding, Comparator<? super K> comparator)
	{
		this(db, dbcomparator, keyBinding, valueBinding, comparator, null, null);
	}

	protected LevelDBStoredSortedMap(DB db, DBComparator dbcomparator, EntryBinding<K> keyBinding,
			EntryBinding<V> valueBinding, Comparator<? super K> comparator, byte[] start, byte[] end)
	{
		super(db, keyBinding, valueBinding);
		this.dbcomparator = dbcomparator;
		this.comparator = comparator;
		this.start = start;
		this.end = end;
	}

	@Override
	public V get(Object key)
	{
		if (!isKeyWithinBounds(byteKey(key)))
		{
			return null;
		}

		return super.get(key);
	}

	@Override
	public V put(K key, V value)
	{
		if (!isKeyWithinBounds(byteKey(key)))
		{
			throw new IllegalArgumentException();
		}

		return super.put(key, value);
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m)
	{
		try (WriteBatch batch = db.createWriteBatch())
		{
			for (java.util.Map.Entry<? extends K, ? extends V> entry : m.entrySet())
			{
				byte[] byteKey = byteKey(entry.getKey());
				if (!isKeyWithinBounds(byteKey))
				{
					throw new IllegalArgumentException();
				}
				batch.put(byteKey, byteValue(entry.getValue()));
			}

			db.write(batch);
		}
		catch (IOException e)
		{
		}
	}

	@Override
	public V remove(Object key)
	{
		if (!isKeyWithinBounds(byteKey(key)))
		{
			return null;
		}

		return super.remove(key);
	}

	protected boolean isKeyWithinBounds(byte[] byteKey)
	{
		if (start != null && dbcomparator.compare(start, byteKey) > 0)
		{
			return false;
		}
		if (end != null && dbcomparator.compare(end, byteKey) < 0)
		{
			return false;
		}
		return true;
	}

	@Override
	public Comparator<? super K> comparator()
	{
		return comparator;
	}

	protected byte[] firstByteKey()
	{
		try (DBIterator i = getDBIterator())
		{
			i.seekToFirst();
			if (i.hasNext())
			{
				return i.next().getKey();
			}
		}
		catch (IOException e)
		{
		}
		throw new NoSuchElementException();
	}

	@Override
	public K firstKey()
	{
		return keyBinding.deserialize(firstByteKey());
	}

	@Override
	public SortedMap<K, V> headMap(Object key)
	{
		return new LevelDBStoredSortedMap<K, V>(db, dbcomparator, keyBinding, valueBinding, comparator, firstByteKey(),
				byteKey(key));
	}

	@Override
	public K lastKey()
	{
		byte[] result = null;
		try (DBIterator i = getDBIterator())
		{
			for (i.seekToFirst(); i.hasNext(); i.next())
			{
				result = i.peekNext().getKey();
			}
		}
		catch (IOException e)
		{
		}

		if (result == null)
		{
			throw new NoSuchElementException();
		}
		return keyBinding.deserialize(result);
	}

	@Override
	public SortedMap<K, V> subMap(Object key1, Object key2)
	{
		byte[] byteKey1 = byteKey(key1);
		byte[] byteKey2 = byteKey(key2);

		if (dbcomparator.compare(byteKey1, byteKey2) > 0)
		{
			throw new IllegalArgumentException();
		}

		return new LevelDBStoredSortedMap<K, V>(db, dbcomparator, keyBinding, valueBinding, comparator, byteKey(key1),
				byteKey(key2));
	}

	@Override
	public SortedMap<K, V> tailMap(Object key)
	{
		return new LevelDBStoredSortedMap<K, V>(db, dbcomparator, keyBinding, valueBinding, comparator, byteKey(key),
				null);
	}

	@Override
	protected DBIterator getDBIterator()
	{
		if (start == null && end == null)
		{
			return db.iterator();
		}
		else
		{
			return new PartitionedDBIterator(db.iterator(), start, end);
		}
	}

	public static class BindedDBComparator<K> extends SimpleDBComparator
	{

		private final EntryBinding<K> keyBinding;

		public BindedDBComparator(EntryBinding<K> keyBinding)
		{
			this.keyBinding = keyBinding;
		}

		@Override
		public int compare(byte[] key1, byte[] key2)
		{
			@SuppressWarnings("unchecked")
			Comparable<K> k1 = (Comparable<K>) keyBinding.deserialize(key1);
			K k2 = (K) keyBinding.deserialize(key2);

			return k1.compareTo(k2);
		}

		@Override
		public String name()
		{
			// TODO: Investigate better naming
			return "BindedDBComparator";
		}

	}

	public static class WrappedDBComparator<K> extends SimpleDBComparator
	{

		private final EntryBinding<K> keyBinding;
		private final Comparator<? super K> comparator;

		public WrappedDBComparator(EntryBinding<K> keyBinding, Comparator<? super K> comparator)
		{
			this.keyBinding = keyBinding;
			this.comparator = comparator;
		}

		@Override
		public int compare(byte[] key1, byte[] key2)
		{
			K k1 = (K) keyBinding.deserialize(key1);
			K k2 = (K) keyBinding.deserialize(key2);

			return comparator.compare(k1, k2);
		}

		@Override
		public String name()
		{
			// TODO: Investigate better naming
			return "WrappedDBComprator";
		}

	}

	/**
	 * Wraps a DBIterator but only exposes elements from the specified start to end
	 * @author Niels Slot <niels@gridline.nl>
	 */
	private class PartitionedDBIterator extends ForwardingDBIterator
	{

		private final DBIterator delegate;
		private final byte[] start;
		private final byte[] end;

		public PartitionedDBIterator(DBIterator iterator, byte[] start, byte[] end)
		{
			delegate = iterator;
			this.start = start;
			this.end = end;
		}

		@Override
		protected DBIterator delegate()
		{
			return delegate;
		}

		@Override
		public boolean hasNext()
		{
			// If the delegate doesn't have more, we can't offer either
			if (!delegate.hasNext())
			{
				return false;
			}

			// We have no end and delegate has elements, so we have too
			if (end == null)
			{
				return true;
			}

			// Check if the next key is within our range
			byte[] next = delegate.peekNext().getKey();

			return dbcomparator.compare(next, end) < 0;
		}

		@Override
		public Entry<byte[], byte[]> next()
		{
			if (!hasNext())
			{
				throw new NoSuchElementException();
			}
			return super.next();
		}

		@Override
		public void seek(byte[] key)
		{
			// If we have a start and key is before it
			if (start != null && dbcomparator.compare(start, key) > 0)
			{
				// Seek to the first element that we have
				seekToFirst();
				return;
			}

			// If we have an end and key is after it
			if (end != null && dbcomparator.compare(end, key) < 0)
			{
				// Seek to the end
				super.seek(end);
				return;
			}

			super.seek(key);
		}

		@Override
		public void seekToFirst()
		{
			if (start == null)
			{
				delegate.seekToFirst();
			}
			else
			{
				delegate.seek(start);
			}
		}

		@Override
		public Entry<byte[], byte[]> peekNext()
		{
			if (!hasNext())
			{
				throw new NoSuchElementException();
			}

			return super.peekNext();
		}

	}

}
