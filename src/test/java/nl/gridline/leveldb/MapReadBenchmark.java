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
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import nl.gridline.leveldb.bindings.StringBinding;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.util.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;

/**
 * @author <a href="mailto:niels@gridline.nl">Niels Slot</a>
 */
public class MapReadBenchmark extends AbstractBenchmark
{

	private static DB db;
	private static LevelDBStoredMap<String, String> map;
	private static File directory;

	@BeforeClass
	public static void createFullMap() throws IOException
	{
		directory = Files.createTempDirectory(null).toFile();

		Options options = new Options();
		options.createIfMissing(true);
		EntryBinding<String> stringBinding = new StringBinding();
		db = factory.open(directory, options);

		map = new LevelDBStoredMap<String, String>(db, stringBinding, stringBinding);

		for (int i = 0; i < 100000; i++)
		{
			String key = i + "key";
			String value = "value" + i;
			db.put(key.getBytes(), value.getBytes());
		}
	}

	@AfterClass
	public static void closeDB() throws IOException
	{
		db.close();
		FileUtils.deleteRecursively(directory);
	}

	@Test
	public void testGet()
	{
		for (int i = 0; i < 100000; i++)
		{
			String key = i + "key";
			String expectedValue = "value" + i;
			String value = map.get(key);
			assertEquals(expectedValue, value);
		}
	}
}
