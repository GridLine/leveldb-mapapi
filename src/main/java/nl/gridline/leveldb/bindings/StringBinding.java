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

import nl.gridline.leveldb.EntryBinding;

/**
 * Implementation of {@link EntryBinding} for Strings. Uses getBytes() and
 * String(byte[]).
 * @author <a href="mailto:niels@gridline.nl">Niels Slot</a>
 */
public class StringBinding implements EntryBinding<String>
{
	@Override
	public byte[] serialize(String object)
	{
		return object.getBytes();
	}

	@Override
	public String deserialize(byte[] object)
	{
		return new String(object);
	}

}
