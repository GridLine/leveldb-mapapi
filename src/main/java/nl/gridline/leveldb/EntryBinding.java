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

/**
 * {@link StoredMap} entry (de)serializer
 * @author <a href="mailto:job@gridline.nl">Job</a>
 * @param <K> object type to (de)serialize
 */
public interface EntryBinding<K>
{
	/**
	 * Serializes {@code object} into a byte array
	 * @param object some object
	 * @return non null byte array
	 */
	byte[] serialize(K object);

	/**
	 * Deserializes {@code object} into {@code K}
	 * @param object non null byte array
	 * @return the original object, can be null if the byte array is empty
	 */
	K deserialize(byte[] object);
}
