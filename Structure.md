# SStable Project

Production-grade, modular SStable implementation in Java. Supports DiskSStable and FlatSStable, block-level compression, Bloom filters, segmented/striped LRU cache, and sharded immutable SSTables.

## Project Structure


## Modules

- **core**: Common SSTable abstractions, key/value entries, compression types, shard manager.
- **disk**: DiskSStable implementation with block-based storage, index, footer, block compression, and safe flush.
- **flat**: FlatSStable implementation optimized for fast random lookups, per-value compression, in-memory indices.
- **cache**: Segmented/striped LRU block cache, supports concurrency and explicit direct buffer freeing.
- **bloom**: Bloom filter implementation for SSTable blocks with writer and reader.
- **tests**: Unit tests covering DiskSStable, FlatSStable, BlockCache, and BloomFilter.

## Features

- Immutable, sharded SSTables (`myfile-%05d-of-%05d`) for large-scale deployment.
- DiskSStable:
    - Block-level compression (Zstd, Flate, Prefix)
    - CRC32C per block
    - Restart points
    - Multi-level index
    - Bloom filter sidecar
    - Atomic flush via temporary file + fsync + rename
- FlatSStable:
    - Optimized random access
    - Top/middle/bottom in-memory index
    - Optional per-value compression
    - Sharding support
- Segmented/striped LRU block cache with optional explicit freeing
- Modular Maven structure for production-scale development

## Build

```bash
# Build all modules
mvn clean install
