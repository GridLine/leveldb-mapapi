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

import java.io.Serializable;

/**
 * @author <a href="mailto:job@gridline.nl">Job</a>
 */
public class SmallObjectSerializable implements Serializable
{

	/**
	 * 
	 */
	private static final long serialVersionUID = -6779707264700567264L;

	private String s;
	private int i;
	private long l;

	public SmallObjectSerializable()
	{

	}

	public static SmallObjectSerializable create(String s, int i, long l)
	{
		final SmallObjectSerializable r = new SmallObjectSerializable();
		r.s = s;
		r.i = i;
		r.l = l;
		return r;
	}

	public String getS()
	{
		return s;
	}

	public int getI()
	{
		return i;
	}

	public long getL()
	{
		return l;
	}
}
