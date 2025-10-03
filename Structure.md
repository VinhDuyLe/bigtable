SStable/
├── pom.xml                  # Parent POM
├── core/
│    ├── pom.xml
│    └── src/main/java/com/sstable/core/
│         ├── SStable.java
│         ├── SStableShardManager.java
│         ├── KeyValueEntry.java
│         └── CompressionType.java
│
├── disk/
│    ├── pom.xml
│    └── src/main/java/com/sstable/disk/
│         ├── DiskSStable.java
│         ├── DiskSStableWriter.java
│         ├── DiskSStableReader.java
│         ├── Block.java
│         ├── BlockBuilder.java
│         ├── BlockIndex.java
│         └── Footer.java
│
├── flat/
│    ├── pom.xml
│    └── src/main/java/com/sstable/flat/
│         ├── FlatSStable.java
│         ├── FlatSStableWriter.java
│         ├── FlatSStableReader.java
│         └── FlatIndex.java
│
├── cache/
│    ├── pom.xml
│    └── src/main/java/com/sstable/cache/
│         ├── BlockCache.java
│         └── SegmentedLRUCache.java
│
├── bloom/
│    ├── pom.xml
│    └── src/main/java/com/sstable/bloom/
│         ├── BloomFilter.java
│         ├── BloomFilterWriter.java
│         └── BloomFilterReader.java
│
├── tests/
│    ├── pom.xml
│    └── src/test/java/com/sstable/tests/
│         ├── DiskSStableTest.java
│         ├── FlatSStableTest.java
│         ├── BlockCacheTest.java
│         └── BloomFilterTest.java
