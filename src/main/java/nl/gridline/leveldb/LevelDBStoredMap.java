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
import java.io.Serializable;
import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.WriteBatch;

/**
 * A Map implementation on top of LevelDB.
 *
 * Note that the following methods (besides the usual suspects) are O(n):
 *
 * <ul>
 *     <li>{@link #clear()}</li>
 *     <li>{@link #size()} (use {@link #isEmpty()} rather than <i>size() == 0)</i></li>
 *     <li>All methods of the object returned by {@link #entrySet()} (except obtaining an iterator).</li>
 *     <li>All methods of the object returned by {@link #keySet()} (except obtaining an iterator).</li>
 *     <li>All methods of the object returned by {@link #values()} (except obtaining an iterator).</li>
 * </ul>
 *
 * @author <a href="mailto:niels@gridline.nl">Niels Slot</a>
 */
public class LevelDBStoredMap<K, V> implements StoredMap<K, V>
{

	protected DB db;
	protected final EntryBinding<K> keyBinding;
	protected final EntryBinding<V> valueBinding;

	public LevelDBStoredMap(DB db, EntryBinding<K> keyBinding, EntryBinding<V> valueBinding)
	{
		this.db = db;
		this.keyBinding = keyBinding;
		this.valueBinding = valueBinding;
	}

	@Override
	public void clear()
	{
		try (WriteBatch batch = db.createWriteBatch())
		{
			try (DBIterator i = getDBIterator())
			{
				for (i.seekToFirst(); i.hasNext(); i.next())
				{
					batch.delete(i.peekNext().getKey());
				}
			}
			db.write(batch);
		}
		catch (IOException e)
		{
		}
	}

	/**
	 * Close the underlying LevelDB database. This should only be called if
	 * the database will not be closed in another way. After this method
	 * has been called this Map cannot be used anymore.
	 */
	@Override
	public void close() throws IOException
	{
		db.close();
		db = null;
	}

	@Override
	public boolean containsKey(Object key)
	{
		return db.get(byteKey(key)) != null;
	}

	@Override
	public boolean containsValue(Object value)
	{
		byte[] byteValue = byteValue(value);
		try (DBIterator i = getDBIterator())
		{
			for (i.seekToFirst(); i.hasNext(); i.next())
			{
				if (Arrays.equals(byteValue, i.peekNext().getValue()))
				{
					return true;
				}
			}
		}
		catch (IOException e)
		{
		}

		return false;
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet()
	{
		return new EntrySet();
	}

	@Override
	public boolean equals(Object obj)
	{
		if (!(obj instanceof Map))
		{
			return false;
		}
		Map<?, ?> other = (Map<?, ?>) obj;
		return entrySet().equals(other.entrySet());
	}

	@Override
	public V get(Object key)
	{
		return getByteKey(byteKey(key));
	}

	protected V getByteKey(byte[] key)
	{
		byte[] rawObject = db.get(key);
		if (rawObject == null)
		{
			return null;
		}
		else
		{
			return valueBinding.deserialize(rawObject);
		}
	}

	@Override
	public int hashCode()
	{
		int hash = 0;
		for (Entry<K, V> entry : entrySet())
		{
			hash += entry.hashCode();
		}
		return hash;
	}

	@Override
	public boolean isEmpty()
	{
		try (DBIterator i = getDBIterator())
		{
			i.seekToFirst();
			return !i.hasNext();
		}
		catch (IOException e)
		{
		}
		return true;
	}

	@Override
	public Set<K> keySet()
	{
		return new KeySet();
	}

	@Override
	public V put(K key, V value)
	{
		if (key == null || value == null)
		{
			throw new NullPointerException();
		}

		byte[] byteKey = byteKey(key);
		V oldValue = getByteKey(byteKey);
		db.put(byteKey, byteValue(value));

		return oldValue;
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m)
	{
		try (WriteBatch batch = db.createWriteBatch())
		{
			for (java.util.Map.Entry<? extends K, ? extends V> entry : m.entrySet())
			{
				batch.put(byteKey(entry.getKey()), byteValue(entry.getValue()));
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
		if (key == null)
		{
			throw new NullPointerException();
		}

		byte[] byteKey = byteKey(key);
		V oldValue = getByteKey(byteKey);
		db.delete(byteKey);

		return oldValue;
	}

	@Override
	public int size()
	{
		int c = 0;
		try (DBIterator i = getDBIterator())
		{
			for (i.seekToFirst(); i.hasNext(); i.next())
			{
				c++;
			}
		}
		catch (IOException e)
		{
		}
		return c;
	}

	@Override
	public Collection<V> values()
	{
		return new ValueCollection();
	}

	protected byte[] byteKey(Object key)
	{
		@SuppressWarnings("unchecked")
		final K keyObject = (K) key;
		return keyBinding.serialize(keyObject);
	}

	protected byte[] byteValue(Object value)
	{
		@SuppressWarnings("unchecked")
		final V valueObject = (V) value;
		return valueBinding.serialize(valueObject);
	}

	protected DBIterator getDBIterator()
	{
		return db.iterator();
	}

	protected class RawEntryIterator implements Iterator<Entry<byte[], byte[]>>
	{
		protected byte[] currentKey = null;
		protected boolean performedDelete = false;

		@Override
		public Entry<byte[], byte[]> next()
		{
			Entry<byte[], byte[]> entry = computeNext();
			if (entry == null)
			{
				throw new NoSuchElementException();
			}
			currentKey = entry.getKey();
			performedDelete = false;
			return entry;
		}

		@Override
		public boolean hasNext()
		{
			Entry<byte[], byte[]> entry = computeNext();
			return entry != null;
		}

		protected Entry<byte[], byte[]> computeNext()
		{
			try (DBIterator i = getDBIterator())
			{
				if (currentKey == null)
				{
					i.seekToFirst();
				}
				else
				{
					i.seek(currentKey);
					if (!performedDelete)
					{
						i.next();
					}
				}
				if (!i.hasNext())
				{
					return null;
				}
				return i.peekNext();
			}
			catch (IOException e)
			{
			}
			return null;
		}

		@Override
		public void remove()
		{
			if (performedDelete || currentKey == null)
			{
				throw new IllegalStateException();
			}
			db.delete(currentKey);
			performedDelete = true;

		}
	}

	protected class EntrySet extends AbstractSet<java.util.Map.Entry<K, V>>
	{

		@Override
		public Iterator<java.util.Map.Entry<K, V>> iterator()
		{
			return new EntryIterator();
		}

		@Override
		public int size()
		{
			return LevelDBStoredMap.this.size();
		}

		private class EntryIterator implements Iterator<java.util.Map.Entry<K, V>>
		{
			private final RawEntryIterator rawEntryIterator = new RawEntryIterator();

			@Override
			public boolean hasNext()
			{
				return rawEntryIterator.hasNext();
			}

			@Override
			public java.util.Map.Entry<K, V> next()
			{
				Entry<byte[], byte[]> rawEntry = rawEntryIterator.next();
				return new LevelDBEntry(keyBinding.deserialize(rawEntry.getKey()));
			}

			@Override
			public void remove()
			{
				rawEntryIterator.remove();
			}

			private class LevelDBEntry implements Map.Entry<K, V>, Serializable
			{
				private static final long serialVersionUID = 1L;
				private final K key;

				public LevelDBEntry(K key)
				{
					this.key = key;
				}

				@Override
				public K getKey()
				{
					return key;
				}

				@Override
				public V setValue(V value)
				{
					return LevelDBStoredMap.this.put(key, value);
				}

				@Override
				public V getValue()
				{
					return LevelDBStoredMap.this.get(key);
				}

				@Override
				public int hashCode()
				{
					V value = getValue();
					return (key == null ? 0 : key.hashCode()) ^ (value == null ? 0 : value.hashCode());
				}

				@Override
				public boolean equals(Object o)
				{
					if (!(o instanceof Map.Entry))
					{
						return false;
					}

					@SuppressWarnings("rawtypes")
					Map.Entry other = (Map.Entry) o;
					V value = getValue();

					return (key == null ? other.getKey() == null : key.equals(other.getKey()))
							&& (value == null ? other.getValue() == null : value.equals(other.getValue()));
				}
			}

		}

	}

	protected class KeySet extends AbstractSet<K>
	{

		@Override
		public Iterator<K> iterator()
		{
			return new KeyIterator();
		}

		@Override
		public int size()
		{
			return LevelDBStoredMap.this.size();
		}

		private class KeyIterator implements Iterator<K>
		{
			private final RawEntryIterator rawEntryIterator = new RawEntryIterator();

			@Override
			public boolean hasNext()
			{
				return rawEntryIterator.hasNext();
			}

			@Override
			public K next()
			{
				return keyBinding.deserialize(rawEntryIterator.next().getKey());
			}

			@Override
			public void remove()
			{
				rawEntryIterator.remove();
			}
		}
	}

	protected class ValueCollection extends AbstractCollection<V>
	{

		@Override
		public Iterator<V> iterator()
		{
			return new ValueIterator();
		}

		@Override
		public int size()
		{
			return LevelDBStoredMap.this.size();
		}

		private class ValueIterator implements Iterator<V>
		{
			private final RawEntryIterator rawEntryIterator = new RawEntryIterator();

			@Override
			public boolean hasNext()
			{
				return rawEntryIterator.hasNext();
			}

			@Override
			public V next()
			{
				return valueBinding.deserialize(rawEntryIterator.next().getValue());
			}

			@Override
			public void remove()
			{
				rawEntryIterator.remove();
			}

		}

	}

}
