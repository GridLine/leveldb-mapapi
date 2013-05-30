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

package nl.gridline.leveldb.bindings;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import nl.gridline.leveldb.EntryBinding;
import nl.gridline.leveldb.exceptions.ObjectExternalizeException;
import nl.gridline.leveldb.io.FastInputStream;
import nl.gridline.leveldb.io.FastOutputStream;

/**
 * Simple implementation of {@link EntryBinding} serializes objects using {@link ObjectInputStream} and
 * {@link ObjectOutputStream}. It relies on the {@link Externalizable} interface to read and write the object. This is
 * much faster than normal {@link Serializable}. See example code <a
 * href="http://www.javacodegeeks.com/2010/07/java-best-practices-high-performance.html">here</a>
 * @author <a href="mailto:job@gridline.nl">Job</a>
 */
public class ObjectExternalizableBinding<K extends Externalizable> implements EntryBinding<K>
{

	private final Class<K> clazz;

	public ObjectExternalizableBinding(Class<K> clazz)
	{
		this.clazz = clazz;
	}

	@Override
	public byte[] serialize(K object)
	{
		FastOutputStream b = new FastOutputStream();
		try (ObjectOutputStream o = new ObjectOutputStream(b))
		{
			object.writeExternal(o);
		}
		catch (IOException e)
		{
			throw new ObjectExternalizeException("Failed to serialize object", e);
		}
		return b.toArray();
	}

	@Override
	public K deserialize(byte[] bytes)
	{
		FastInputStream b = new FastInputStream(bytes);
		try (ObjectInputStream o = new ObjectInputStream(b))
		{
			final K result = clazz.newInstance();
			result.readExternal(o);
			return result;
		}
		catch (IOException | InstantiationException | IllegalAccessException | ClassNotFoundException e)
		{
			throw new ObjectExternalizeException("Failed to deserialize object", e);
		}
	}
}
