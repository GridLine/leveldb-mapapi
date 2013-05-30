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

import static org.iq80.leveldb.impl.Iq80DBFactory.factory;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;

import nl.gridline.leveldb.bindings.StringBinding;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBComparator;
import org.iq80.leveldb.Options;

/**
 * Provides various factory methods to create both LevelDBStoredMap and LevelDBStoredSortedMap.
 * @author <a href="mailto:niels@gridline.nl">Niels Slot</a>
 */
public class LevelDBMapFactory
{

	/**
	 * Returns a StoredMap which is backed by the db
	 * @param db The DB instance to use
	 * @param keyBinding An EntryBinding implementation which is used to convert the keys
	 * @param valueBinding An EntryBinding implementation which is used to convert the values
	 * @return A StoredMap. This Map can only work as long as the db is not closed.
	 */
	public static <K, V> StoredMap<K, V> createMapForDB(DB db, EntryBinding<K> keyBinding, EntryBinding<V> valueBinding)
	{
		return new LevelDBStoredMap<K, V>(db, keyBinding, valueBinding);
	}

	/**
	 * Returns a StoredMap which is backed by the db and uses Strings as keys
	 * @param db The DB instance to use
	 * @param valueBinding An EntryBinding implementation which is used to convert the values
	 * @return A StoredMap. This Map can only work as long as the db is not closed.
	 */
	public static <V> StoredMap<String, V> createMapForDB(DB db, EntryBinding<V> valueBinding)
	{
		return createMapForDB(db, new StringBinding(), valueBinding);
	}

	/**
	 * Returns a StoredMap which is backed by a DB in the specified directory
	 * @param directory The directory in which the database is or will be created
	 * @param keyBinding An EntryBinding implementation which is used to convert the keys
	 * @param valueBinding An EntryBinding implementation which is used to convert the values
	 * @return A StoredMap. The user is responsible for calling close() when the map is no longer needed.
	 * @throws IOException
	 */
	public static <K, V> StoredMap<K, V> createMap(File directory, EntryBinding<K> keyBinding,
			EntryBinding<V> valueBinding) throws IOException
	{
		Options options = new Options();
		options.createIfMissing(true);

		DB db = factory.open(directory, options);

		return createMapForDB(db, keyBinding, valueBinding);
	}

	/**
	 * Returns a StoredMap which is backed by a DB in the specified directory and uses Strings as keys
	 * @param directory The directory in which the database is or will be created
	 * @param valueBinding An EntryBinding implementation which is used to convert the values
	 * @return A StoredMap. The user is responsible for calling close() when the map is no longer needed.
	 * @throws IOException
	 */
	public static <V> StoredMap<String, V> createMap(File directory, EntryBinding<V> valueBinding) throws IOException
	{
		return createMap(directory, new StringBinding(), valueBinding);
	}

	/**
	 * Returns a StoredSortedMap which is backed by the db and sorted using dbcomparator
	 * @param db The DB instance to use
	 * @param dbcomparator The DBComparator that was used to open the db. This DBComparator must match the comparator
	 * @param keyBinding An EntryBinding implementation which is used to convert the keys
	 * @param valueBinding An EntryBinding implementation which is used to convert the values
	 * @param comparator The mandatory comparator which needs to be supplied to a SortedMap. This implementation does not
	 *           use it, instead make sure the DBComparator behaves correctly.
	 * @return A StoredSortedMap. This Map can only work as long as the db is not closed.
	 */
	public static <K, V> StoredSortedMap<K, V> createSortedMapForDB(DB db, DBComparator dbcomparator,
			EntryBinding<K> keyBinding, EntryBinding<V> valueBinding, Comparator<? super K> comparator)
	{
		return new LevelDBStoredSortedMap<K, V>(db, dbcomparator, keyBinding, valueBinding, comparator);
	}

	/**
	 * Returns a StoredSortedMap which is backed by the db and sorted using dbcomparator
	 * @param db The DB instance to use
	 * @param dbcomparator The DBComparator that was used to open the db. This DBComparator must match the natural
	 *           ordering of the keys.
	 * @param keyBinding An EntryBinding implementation which is used to convert the keys
	 * @param valueBinding An EntryBinding implementation which is used to convert the values
	 * @return A StoredSortedMap. This Map can only work as long as the db is not closed.
	 */
	public static <K, V> StoredSortedMap<K, V> createSortedMapForDB(DB db, DBComparator dbcomparator,
			EntryBinding<K> keyBinding, EntryBinding<V> valueBinding)
	{
		return createSortedMapForDB(db, dbcomparator, keyBinding, valueBinding, null);
	}

	/**
	 * Returns a StoredSortedMap which is backed by a DB in the specified directory and sorted using the natural order of
	 * the keys.
	 * @param directory The directory in which the database is or will be created
	 * @param keyBinding An EntryBinding implementation which is used to convert the keys
	 * @param valueBinding An EntryBinding implementation which is used to convert the values
	 * @return A StoredSortedMap. The user is responsible for calling close() when the map is no longer needed.
	 * @throws IOException
	 */
	public static <K, V> StoredSortedMap<K, V> createSortedMap(File directory, EntryBinding<K> keyBinding,
			EntryBinding<V> valueBinding) throws IOException
	{
		DBComparator dbcomparator = new LevelDBStoredSortedMap.BindedDBComparator<K>(keyBinding);

		Options options = new Options();
		options.createIfMissing(true);
		options.comparator(dbcomparator);

		DB db = factory.open(directory, options);

		return createSortedMapForDB(db, dbcomparator, keyBinding, valueBinding);
	}

	/**
	 * Returns a StoredSortedMap which is backed by a DB in the specified directory and sorted using the comparator.
	 * Warning: do not open the same database with a different comparator!
	 * @param directory The directory in which the database is or will be created
	 * @param keyBinding An EntryBinding implementation which is used to convert the keys
	 * @param valueBinding An EntryBinding implementation which is used to convert the values
	 * @param comparator The comparator that is used to sort the map
	 * @return A StoredSortedMap. The user is responsible for calling close() when the map is no longer needed.
	 * @throws IOException
	 */
	public static <K, V> StoredSortedMap<K, V> createSortedMap(File directory, EntryBinding<K> keyBinding,
			EntryBinding<V> valueBinding, Comparator<? super K> comparator) throws IOException
	{
		DBComparator dbcomparator = new LevelDBStoredSortedMap.WrappedDBComparator<K>(keyBinding, comparator);

		Options options = new Options();
		options.createIfMissing(true);
		options.comparator(dbcomparator);

		DB db = factory.open(directory, options);

		return createSortedMapForDB(db, dbcomparator, keyBinding, valueBinding, comparator);
	}

	/**
	 * Returns a StoredSortedMap which is backed by a DB in the specified directory and uses Strings as keys
	 * @param directory The directory in which the database is or will be created
	 * @param valueBinding An EntryBinding implementation which is used to convert the values
	 * @return A StoredSortedMap. The user is responsible for calling close() when the map is no longer needed.
	 * @throws IOException
	 */
	public static <V> StoredSortedMap<String, V> createSortedMap(File directory, EntryBinding<V> valueBinding)
			throws IOException
	{
		EntryBinding<String> keyBinding = new StringBinding();
		return createSortedMap(directory, keyBinding, valueBinding);
	}

}
