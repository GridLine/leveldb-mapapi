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

import static nl.gridline.leveldb.bindings.utils.Bytes.toBytes;
import static nl.gridline.leveldb.bindings.utils.Bytes.toFloat;
import nl.gridline.leveldb.EntryBinding;

/**
 * Binding for float values.
 * @author <a href="mailto:job@gridline.nl">Job</a>
 */
public class FloatBinding implements EntryBinding<Float>
{

	@Override
	public byte[] serialize(Float object)
	{
		return toBytes(object.floatValue());
	}

	@Override
	public Float deserialize(byte[] object)
	{
		return toFloat(object);
	}

}
