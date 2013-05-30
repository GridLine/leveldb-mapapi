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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import nl.gridline.leveldb.bindings.StringBinding;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.util.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;

public class MapBenchmark extends AbstractBenchmark
{

	private LevelDBStoredMap<String, String> map = null;
	private DB db = null;
	private File directory;

	@Before
	public void createMap() throws IOException
	{
		directory = Files.createTempDirectory(null).toFile();

		Options options = new Options();
		options.createIfMissing(true);
		EntryBinding<String> stringBinding = new StringBinding();

		db = org.iq80.leveldb.impl.Iq80DBFactory.factory.open(directory, options);

		map = new LevelDBStoredMap<String, String>(db, stringBinding, stringBinding);
	}

	@After
	public void closeDB() throws IOException
	{
		db.close();
		FileUtils.deleteRecursively(directory);
	}

	@Test
	public void testPut()
	{
		for (int i = 0; i < 100000; i++)
		{
			String key = i + "key";
			String value = "value" + i;
			map.put(key, value);
		}
	}

	@Test
	public void testDirectPut()
	{
		for (int i = 0; i < 100000; i++)
		{
			String key = i + "key";
			String value = "value" + i;
			db.put(key.getBytes(), value.getBytes());
		}
	}

}
