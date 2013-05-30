# Map API for LevelDB

This is an implementation of java.util.Map and java.util.SortedMap on top of
LevelDB. Everything stored in these maps will be persistent and saved to
disk in a LevelDB database.

## Status

Both the java.util.Map and java.util.SortedMap interface are implemented.
We pass both the MapInterfaceTest and SortedMapInterface tests from Guava.
Please report an [issue](https://github.com/GridLine/leveldb-mapapi/issues)
if you encounter any problems.

## Usage

### Map

Create a Map with Strings as keys and values backed by LevelDB:

	StoredMap<String, String> map = LevelDBMapFactory.createMap(new File("/path/to/directory"), new StringBinding());
	map.put("key", "value");
	map.put("key2", "value2");
	
	// Print "value"
	System.out.println(map.get("key"));
	
	// Close the database
	map.close();
	
	// Reopen the database from disk
	map = LevelDBMapFactory.createMap(new File("/path/to/directory"), new StringBinding()))
	
	// Print "value2"
	System.out.println(map.get("key2"));
	
	// Close the database again
	map.close();
	
Note that you have to call close on the map explicitly since its LevelDB database needs to be closed.

The StoredMap is an implementation of java.util.Map and
can be used as such:

	Map<String, String> readOnly = Collections.unmodifiableMap(map);
	
It is possible to use any serializable object as a value:

	StoredMap<String, Integer> map = LevelDBMapFactory.createMap(new File("/path/to/directory"), new ObjectSerializableBinding<Integer>());
	map.put("key", 10);
	map.put("key2", new Integer(5));
	
	int count = 0;
	for(Map.Entry<String, Integer> entry : map.entrySet())
	{
		System.out.println(entry.getKey());
		count += entry.getValue();
	}
	// Print 15
	System.out.println(count);
	
	map.close();
	
	try(StoredMap<String, POJO> pojoMap = LevelDBMapFactory.createMap(
			new File("/path/to/directory"), new ObjectSerializableBinding<POJO>()))
	{
		pojoMap.put("key", new POJO());
	}
	
Serializable objects can also be used as keys:

	StoredMap<Student, Grade> grades = LevelDBMapFactory.createMap(new File("/path/to/directory"),
		new ObjectSerializableBinding<Student>(), new ObjectSerializableBinding<Grade>());
	grades.put(john, Grade.A);
	grades.put(sam, Grade.C);
	
	grades.close();
	
### SortedMap

The SortedMap interface is also supported through a set of factory methods:

	// Create a SortedMap using Strings as Comparable keys
	StoredSortedMap<String, String> dict = LevelDBMapFactory.createSortedMap(new File("/path/to/directory"), new StringBinding());
	dict.put("zwaaien", "to wave");
	dict.put("boek", "book");
	
	// Print "boek" and then "zwaaien"
	for(String key : dict.keySet())
	{
		System.out.println(key);
	}
	
	dict.close();
	
It is possible to supply a java.util.Comparator:

	StoredSortedMap<String, String> reverseSortedMap = LevelDBMapFactory.createSortedMap(new File("/path/to/directory"),
		new StringBinding(), new StringBinding(), new Comparator<String>()
		{
			@Override
			public int compare(String string1, String string2)
			{
				return string2.compareTo(string1);
			}
		});
	reverseSortedMap.put("Amsterdam", "Europe");
	reverseSortedMap.put("Washington", "North America");
	reverseSortedMap.put("Tokyo", "Asia");
	
	// Print "Washington", "Tokyo" and finally "Amsterdam"
	for(String key : reverseSortedMap.keySet())
	{
		System.out.println(key);
	}
	
	reverseSortedMap.close();
	
### Wrapping an existing LevelDB instance

An open instance of a LevelDB object can also be used to create a Map:

	Options options = new Options();
	options.createIfMissing(true);
	DB db = Iq80DBFactory.factory.open(new File("/path/to/directory"), options);
	db.put(new String("key").getBytes(), new String("value").getBytes());
	
	EntryBinding<String> stringBinding = new StringBinding();
	Map<String, String> map = LevelDBMapFactory.createMapForDB(db, stringBinding, stringBinding);
	
	// Print "value"
	System.out.println(map.get("key"));
	
	map.close();
	
