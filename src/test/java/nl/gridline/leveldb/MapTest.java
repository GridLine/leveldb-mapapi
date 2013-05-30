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
import java.nio.file.Files;
import java.util.Map;

import nl.gridline.leveldb.bindings.StringBinding;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.util.FileUtils;

import com.google.common.collect.testing.MapInterfaceTest;

/**
 * @author <a href="mailto:niels@gridline.nl">Niels Slot</a>
 */
public class MapTest extends MapInterfaceTest<String, String>
{

	private File empty;
	private File populated;

	public MapTest()
	{
		super(false, false, true, true, true);
	}

	@Override
	protected void setUp() throws Exception
	{
		super.setUp();
		empty = Files.createTempDirectory(null).toFile();
		populated = Files.createTempDirectory(null).toFile();
	}

	@Override
	protected void tearDown() throws Exception
	{
		super.tearDown();
		FileUtils.deleteRecursively(empty);
		FileUtils.deleteRecursively(populated);
	}

	@Override
	protected String getKeyNotInPopulatedMap() throws UnsupportedOperationException
	{
		return "test2";
	}

	@Override
	protected String getValueNotInPopulatedMap() throws UnsupportedOperationException
	{
		return "test123";
	}

	@Override
	protected Map<String, String> makeEmptyMap() throws UnsupportedOperationException
	{
		FileUtils.deleteRecursively(empty);

		Options options = new Options();
		options.createIfMissing(true);
		EntryBinding<String> stringBinding = new StringBinding();
		DB db = null;
		try
		{
			db = factory.open(empty, options);
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}

		return new LevelDBStoredMap<String, String>(db, stringBinding, stringBinding);
	}

	@Override
	protected Map<String, String> makePopulatedMap() throws UnsupportedOperationException
	{
		FileUtils.deleteRecursively(populated);

		Options options = new Options();
		options.createIfMissing(true);
		EntryBinding<String> stringBinding = new StringBinding();
		DB db = null;
		try
		{
			db = factory.open(populated, options);
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}

		LevelDBStoredMap<String, String> result = new LevelDBStoredMap<String, String>(db, stringBinding, stringBinding);
		result.put("test", "gridline");
		result.put("key", "value");
		return result;
	}
}
