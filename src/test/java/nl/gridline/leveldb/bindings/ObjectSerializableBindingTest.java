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

import static org.junit.Assert.assertEquals;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;

/**
 * @author <a href="mailto:job@gridline.nl">Job</a>
 */
public class ObjectSerializableBindingTest
{

	private final String s = "This is a test String";

	@Rule
	public TestRule benchmarkRun = new BenchmarkRule();

	@Test
	@BenchmarkOptions(benchmarkRounds = 20000, warmupRounds = 1000)
	public void testSerializeTwenty() throws Exception
	{
		ObjectSerializableBinding<SmallObjectSerializable> binding = new ObjectSerializableBinding<>();

		SmallObjectSerializable in = SmallObjectSerializable.create(s, 100, 2000L);

		byte[] object = binding.serialize(in);

		SmallObjectSerializable out = binding.deserialize(object);

		assertEquals(in.getS(), out.getS());
		assertEquals(in.getI(), out.getI());
		assertEquals(in.getL(), out.getL());
	}

	@Test
	@BenchmarkOptions(benchmarkRounds = 50000, warmupRounds = 1000)
	public void testSerializeFifty() throws Exception
	{
		ObjectSerializableBinding<SmallObjectSerializable> binding = new ObjectSerializableBinding<>();

		SmallObjectSerializable in = SmallObjectSerializable.create(s, 100, 2000L);

		byte[] object = binding.serialize(in);

		SmallObjectSerializable out = binding.deserialize(object);

		assertEquals(in.getS(), out.getS());
		assertEquals(in.getI(), out.getI());
		assertEquals(in.getL(), out.getL());

	}

	@Test
	@BenchmarkOptions(benchmarkRounds = 100000, warmupRounds = 1000)
	public void testSerializeHundred() throws Exception
	{
		ObjectSerializableBinding<SmallObjectSerializable> binding = new ObjectSerializableBinding<>();

		SmallObjectSerializable in = SmallObjectSerializable.create(s, 100, 2000L);

		byte[] object = binding.serialize(in);

		SmallObjectSerializable out = binding.deserialize(object);

		assertEquals(in.getS(), out.getS());
		assertEquals(in.getI(), out.getI());
		assertEquals(in.getL(), out.getL());

	}

}
