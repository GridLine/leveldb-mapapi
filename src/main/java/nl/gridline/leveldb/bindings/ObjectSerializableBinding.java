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
import nl.gridline.leveldb.exceptions.ObjectSerializeException;
import nl.gridline.leveldb.io.FastInputStream;
import nl.gridline.leveldb.io.FastOutputStream;

/**
 * Implementation of {@link EntryBinding} (de)serializes object using {@link ObjectInputStream} and
 * {@link ObjectOutputStream}. The implementation relies on {@link Serializable} interface to serialize objects.
 * Tests show that a {@link Externalizable} implementation is much faster. If possible use
 * {@link ObjectExternalizableBinding}
 * @author <a href="mailto:job@gridline.nl">Job</a>
 */
public class ObjectSerializableBinding<K extends Serializable> implements EntryBinding<K>
{

	@Override
	public byte[] serialize(K object)
	{
		FastOutputStream b = new FastOutputStream();
		try (ObjectOutputStream o = new ObjectOutputStream(b))
		{
			o.writeObject(object);
			return b.toArray();
		}
		catch (IOException e)
		{
			throw new ObjectSerializeException("Failed to serialize object", e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public K deserialize(byte[] buffer)
	{
		FastInputStream b = new FastInputStream(buffer);
		try (ObjectInputStream i = new ObjectInputStream(b))
		{
			Object r = i.readObject();
			return (K) r;
		}
		catch (IOException | ClassNotFoundException e)
		{
			throw new ObjectSerializeException("Failed to deserialize object", e);
		}
	}
}
