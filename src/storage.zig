const std = @import("std");
const rocksdb = @import("rocksdb");
const cache = @import("cache.zig");
const internals = @import("internals.zig");

const Allocator = std.mem.Allocator;
const DB = rocksdb.DB;
const Data = rocksdb.Data;
const WriteBatch = rocksdb.WriteBatch;
const ColumnFamilyHandle = rocksdb.ColumnFamilyHandle;
const Iterator = rocksdb.Iterator;

pub const StorageConfig = struct {
    path: []const u8,

    max_open_files: i32 = 20000,

    enable_cache: bool = true,
    cache_size: u32 = 500_000,
    cache_segment_count: u16 = 32,
    cache_ttl: u32 = 3600,

    batch_buffer_size: usize = 100_000,

    write_buffer_size: usize = 128 * 1024 * 1024,
    max_write_buffer_number: i32 = 6,
    target_file_size_base: usize = 128 * 1024 * 1024,
    max_background_jobs: i32 = 8,
    level0_file_num_compaction_trigger: i32 = 4,
    level0_slowdown_writes_trigger: i32 = 24,
    level0_stop_writes_trigger: i32 = 40,
    enable_pipelined_write: bool = true,
    enable_write_thread_adaptive_yield: bool = true,

    use_fast_compression: bool = true,

    pub fn highPerformance() StorageConfig {
        return .{
            .path = "./data",
            .max_open_files = 30000,
            .enable_cache = true,
            .cache_size = 1_000_000,
            .cache_segment_count = 64,
            .batch_buffer_size = 200_000,
            .write_buffer_size = 256 * 1024 * 1024,
            .max_write_buffer_number = 8,
            .max_background_jobs = 16,
        };
    }

    pub fn memoryEfficient() StorageConfig {
        return .{
            .path = "./data",
            .max_open_files = 10000,
            .enable_cache = true,
            .cache_size = 250_000,
            .cache_segment_count = 16,
            .batch_buffer_size = 50_000,
            .write_buffer_size = 64 * 1024 * 1024,
            .max_write_buffer_number = 4,
            .max_background_jobs = 4,
        };
    }
};

pub const VectorMetadata = struct {
    dimensions: usize,
    timestamp: i64,
    collection: []const u8,

    pub fn deinit(self: *VectorMetadata, allocator: Allocator) void {
        allocator.free(self.collection);
    }

    pub fn serialize(self: VectorMetadata, allocator: Allocator) ![]u8 {
        return try std.fmt.allocPrint(
            allocator,
            "{{\"dimensions\":{d},\"timestamp\":{d},\"collection\":\"{s}\"}}",
            .{ self.dimensions, self.timestamp, self.collection },
        );
    }

    pub fn deserialize(allocator: Allocator, data: []const u8) !VectorMetadata {
        var dimensions: usize = 0;
        var timestamp: i64 = 0;
        var collection: []const u8 = "";

        var iter = std.mem.tokenizeAny(u8, data, "{},:\"");
        while (iter.next()) |token| {
            if (std.mem.eql(u8, token, "dimensions")) {
                if (iter.next()) |val| {
                    dimensions = try std.fmt.parseInt(usize, val, 10);
                }
            } else if (std.mem.eql(u8, token, "timestamp")) {
                if (iter.next()) |val| {
                    timestamp = try std.fmt.parseInt(i64, val, 10);
                }
            } else if (std.mem.eql(u8, token, "collection")) {
                if (iter.next()) |val| {
                    collection = val;
                }
            }
        }

        return VectorMetadata{
            .dimensions = dimensions,
            .timestamp = timestamp,
            .collection = try allocator.dupe(u8, collection),
        };
    }
};

const CachedVector = struct {
    data: []const u8,
    allocator: Allocator,

    pub fn removedFromCache(self: *CachedVector, allocator: Allocator) void {
        _ = allocator;
        self.allocator.free(self.data);
    }

    pub fn size(self: CachedVector) u32 {
        return @intCast(self.data.len);
    }
};

pub const StorageError = error{
    DatabaseNotInitialized,
    VectorNotFound,
    InvalidVector,
    DimensionMismatch,
    SerializationFailed,
    DeserializationFailed,
    BatchOperationFailed,
    IteratorError,
    InvalidKey,
} || Allocator.Error;

pub const Storage = struct {
    db: ?DB,
    vectors_cf: ?ColumnFamilyHandle,
    metadata_cf: ?ColumnFamilyHandle,
    index_state_cf: ?ColumnFamilyHandle,
    allocator: Allocator,
    config: StorageConfig,
    vector_cache: ?*cache.Cache(CachedVector),
    metrics: internals.Metrics,

    const Self = @This();

    pub fn init(allocator: Allocator, config: StorageConfig) !Self {
        var vector_cache: ?*cache.Cache(CachedVector) = null;

        if (config.enable_cache) {
            const cache_ptr = try allocator.create(cache.Cache(CachedVector));
            cache_ptr.* = try cache.Cache(CachedVector).init(allocator, .{
                .max_size = config.cache_size,
                .segment_count = config.cache_segment_count,
                .gets_per_promote = 3,
                .shrink_ratio = 0.1,
            });
            vector_cache = cache_ptr;

            std.debug.print("[STORAGE] Cache initialized: size={d}, segments={d}\n", .{
                config.cache_size,
                config.cache_segment_count,
            });
        }

        return Self{
            .db = null,
            .vectors_cf = null,
            .metadata_cf = null,
            .index_state_cf = null,
            .allocator = allocator,
            .config = config,
            .vector_cache = vector_cache,
            .metrics = internals.Metrics.init(),
        };
    }

    pub fn open(self: *Self) !void {
        var err_str: ?Data = null;
        defer if (err_str) |e| e.deinit();

        const db_opened, const families = try DB.open(
            self.allocator,
            self.config.path,
            .{
                .create_if_missing = true,
                .create_missing_column_families = true,
                .max_open_files = self.config.max_open_files,
            },
            &.{
                .{ .name = "default" },
                .{ .name = "vectors" },
                .{ .name = "metadata" },
                .{ .name = "index_state" },
            },
            &err_str,
        );

        self.db = db_opened;
        self.vectors_cf = families[1].handle;
        self.metadata_cf = families[2].handle;
        self.index_state_cf = families[3].handle;

        self.allocator.free(families);

        std.debug.print("[STORAGE] RocksDB opened successfully\n", .{});
    }

    pub fn deinit(self: *Self) void {
        if (self.vector_cache) |cache_ptr| {
            cache_ptr.deinit();
            self.allocator.destroy(cache_ptr);
        }

        if (self.db) |db| {
            db.deinit();
        }
    }

    pub fn putVector(
        self: *Self,
        collection: []const u8,
        id: []const u8,
        vector: []const f32,
    ) !void {
        const start = std.time.nanoTimestamp();
        defer {
            const elapsed = std.time.nanoTimestamp() - start;
            self.metrics.recordInsert();
            _ = elapsed;
        }

        const db = self.db orelse return StorageError.DatabaseNotInitialized;

        const key = try internals.encodeKey(self.allocator, collection, id);
        defer self.allocator.free(key);

        const vector_bytes = try internals.serializeVector(self.allocator, vector);
        defer self.allocator.free(vector_bytes);

        var err_str: ?Data = null;
        defer if (err_str) |e| e.deinit();

        try db.put(self.vectors_cf, key, vector_bytes, &err_str);

        if (self.vector_cache) |cache_ptr| {
            const cached_vector = try self.allocator.dupe(u8, vector_bytes);
            cache_ptr.put(key, .{
                .data = cached_vector,
                .allocator = self.allocator,
            }, .{
                .ttl = self.config.cache_ttl,
            }) catch |err| {
                self.allocator.free(cached_vector);
                return err;
            };
        }

        const metadata = VectorMetadata{
            .dimensions = vector.len,
            .timestamp = std.time.timestamp(),
            .collection = collection,
        };
        const metadata_bytes = try metadata.serialize(self.allocator);
        defer self.allocator.free(metadata_bytes);

        try db.put(self.metadata_cf, key, metadata_bytes, &err_str);
    }

    pub fn getVector(
        self: *Self,
        collection: []const u8,
        id: []const u8,
    ) !?[]f32 {
        const db = self.db orelse return StorageError.DatabaseNotInitialized;

        const key = try internals.encodeKey(self.allocator, collection, id);
        defer self.allocator.free(key);

        if (self.vector_cache) |cache_ptr| {
            if (cache_ptr.get(key)) |entry| {
                defer entry.release();
                self.metrics.recordCacheHit();
                return try internals.deserializeVector(entry.value.data, self.allocator);
            }
            self.metrics.recordCacheMiss();
        }

        var err_str: ?Data = null;
        defer if (err_str) |e| e.deinit();

        const result = try db.get(self.vectors_cf, key, &err_str);
        if (result == null) return null;

        defer result.?.deinit();

        const vector = try internals.deserializeVector(result.?.data, self.allocator);

        if (self.vector_cache) |cache_ptr| {
            const cached_vector = try self.allocator.dupe(u8, result.?.data);
            cache_ptr.put(key, .{
                .data = cached_vector,
                .allocator = self.allocator,
            }, .{
                .ttl = self.config.cache_ttl,
            }) catch {
                self.allocator.free(cached_vector);
            };
        }

        return vector;
    }

    pub fn getMetadata(
        self: *Self,
        collection: []const u8,
        id: []const u8,
    ) !?VectorMetadata {
        const db = self.db orelse return StorageError.DatabaseNotInitialized;

        const key = try internals.encodeKey(self.allocator, collection, id);
        defer self.allocator.free(key);

        var err_str: ?Data = null;
        defer if (err_str) |e| e.deinit();

        const result = try db.get(self.metadata_cf, key, &err_str);
        if (result == null) return null;

        defer result.?.deinit();

        return try VectorMetadata.deserialize(self.allocator, result.?.data);
    }

    pub fn deleteVector(
        self: *Self,
        collection: []const u8,
        id: []const u8,
    ) !void {
        const db = self.db orelse return StorageError.DatabaseNotInitialized;

        const key = try internals.encodeKey(self.allocator, collection, id);
        defer self.allocator.free(key);

        if (self.vector_cache) |cache_ptr| {
            _ = cache_ptr.del(key);
        }

        var err_str: ?Data = null;
        defer if (err_str) |e| e.deinit();

        try db.delete(self.vectors_cf, key, &err_str);
        try db.delete(self.metadata_cf, key, &err_str);

        self.metrics.recordDelete();
    }

    pub fn putVectorBatch(
        self: *Self,
        collection: []const u8,
        ids: []const []const u8,
        vectors: []const []const f32,
    ) !void {
        if (ids.len != vectors.len) return StorageError.BatchOperationFailed;
        if (ids.len == 0) return;

        const db = self.db orelse return StorageError.DatabaseNotInitialized;

        std.debug.print("[STORAGE] Starting batch insert: {d} vectors\n", .{ids.len});

        const timestamp = std.time.timestamp();

        var batch = WriteBatch.init();
        defer batch.deinit();

        var keys_list = try std.ArrayList([]u8).initCapacity(
            self.allocator,
            1000,
        );
        defer {
            for (keys_list.items) |k| self.allocator.free(k);
            keys_list.deinit(self.allocator);
        }

        var vector_bytes_list = try std.ArrayList([]u8).initCapacity(
            self.allocator,
            1000,
        );
        defer {
            for (vector_bytes_list.items) |v| self.allocator.free(v);
            vector_bytes_list.deinit(self.allocator);
        }

        var metadata_bytes_list = try std.ArrayList([]u8).initCapacity(
            self.allocator,
            1000,
        );
        defer {
            for (metadata_bytes_list.items) |m| self.allocator.free(m);
            metadata_bytes_list.deinit(self.allocator);
        }

        for (ids, vectors) |id, vector| {
            const key = try internals.encodeKey(self.allocator, collection, id);
            try keys_list.append(self.allocator, key);

            const vector_bytes = try internals.serializeVector(self.allocator, vector);
            try vector_bytes_list.append(self.allocator, vector_bytes);

            batch.put(self.vectors_cf.?, key, vector_bytes);

            const metadata = VectorMetadata{
                .dimensions = vector.len,
                .timestamp = timestamp,
                .collection = collection,
            };
            const metadata_bytes = try metadata.serialize(self.allocator);
            try metadata_bytes_list.append(self.allocator, metadata_bytes);

            batch.put(self.metadata_cf.?, key, metadata_bytes);

            self.metrics.recordInsert();
        }

        std.debug.print("[STORAGE] Writing batch to RocksDB...\n", .{});

        var err_str: ?Data = null;
        defer if (err_str) |e| e.deinit();

        try db.write(batch, &err_str);
        std.debug.print("[STORAGE] Batch write completed successfully\n", .{});
    }

    pub fn deleteRange(
        self: *Self,
        collection: []const u8,
    ) !usize {
        const db = self.db orelse return StorageError.DatabaseNotInitialized;

        const prefix = try std.fmt.allocPrint(self.allocator, "{s}:", .{collection});
        defer self.allocator.free(prefix);

        if (self.vector_cache) |cache_ptr| {
            _ = try cache_ptr.delPrefix(prefix);
        }

        var count: usize = 0;
        {
            var iter = db.iterator(self.vectors_cf, .forward, prefix);
            defer iter.deinit();

            var err_str: ?Data = null;
            defer if (err_str) |e| e.deinit();

            while (try iter.nextKey(&err_str)) |key| {
                if (!std.mem.startsWith(u8, key.data, prefix)) break;
                count += 1;
            }
        }

        const end_prefix = try std.fmt.allocPrint(self.allocator, "{s};", .{collection});
        defer self.allocator.free(end_prefix);

        var batch = WriteBatch.init();
        defer batch.deinit();

        batch.deleteRange(self.vectors_cf.?, prefix, end_prefix);
        batch.deleteRange(self.metadata_cf.?, prefix, end_prefix);

        var err_str: ?Data = null;
        defer if (err_str) |e| e.deinit();

        try db.write(batch, &err_str);
        try db.flush(self.vectors_cf, &err_str);
        try db.flush(self.metadata_cf, &err_str);

        return count;
    }

    pub const VectorIterator = struct {
        storage: *Self,
        iter: Iterator,
        collection_prefix: []const u8,
        allocator: Allocator,

        pub fn deinit(self: *VectorIterator) void {
            self.iter.deinit();
            self.allocator.free(self.collection_prefix);
        }

        pub fn next(self: *VectorIterator) !?struct { id: []const u8, vector: []f32 } {
            var err_str: ?Data = null;
            defer if (err_str) |e| e.deinit();

            while (try self.iter.next(&err_str)) |entry| {
                const key = entry[0].data;

                if (!std.mem.startsWith(u8, key, self.collection_prefix)) {
                    return null;
                }

                const vector_bytes = entry[1].data;
                const vector = try internals.deserializeVector(vector_bytes, self.allocator);

                const decoded = internals.decodeKey(key) orelse continue;
                const id = try self.allocator.dupe(u8, decoded.id);

                return .{ .id = id, .vector = vector };
            }

            return null;
        }
    };

    pub fn iterateCollection(
        self: *Self,
        collection: []const u8,
    ) !VectorIterator {
        const db = self.db orelse return StorageError.DatabaseNotInitialized;

        const prefix = try std.fmt.allocPrint(self.allocator, "{s}:", .{collection});
        const iter = db.iterator(self.vectors_cf, .forward, prefix);

        return VectorIterator{
            .storage = self,
            .iter = iter,
            .collection_prefix = prefix,
            .allocator = self.allocator,
        };
    }

    pub fn flush(self: *Self) !void {
        const db = self.db orelse return StorageError.DatabaseNotInitialized;

        var err_str: ?Data = null;
        defer if (err_str) |e| e.deinit();

        try db.flush(self.vectors_cf, &err_str);
        try db.flush(self.metadata_cf, &err_str);
    }

    pub fn getStats(self: *Self) !StorageStats {
        const db = self.db orelse return StorageError.DatabaseNotInitialized;

        const num_keys_prop = db.propertyValueCf(self.vectors_cf, "rocksdb.estimate-num-keys");
        defer num_keys_prop.deinit();

        const sst_size_prop = db.propertyValueCf(self.vectors_cf, "rocksdb.total-sst-files-size");
        defer sst_size_prop.deinit();

        const estimated_keys = std.fmt.parseInt(u64, num_keys_prop.data, 10) catch 0;
        const sst_size = std.fmt.parseInt(u64, sst_size_prop.data, 10) catch 0;

        return StorageStats{
            .estimated_vectors = estimated_keys,
            .total_size_bytes = sst_size,
            .cache_hit_rate = self.metrics.getCacheHitRate(),
            .insert_count = self.metrics.getInsertCount(),
            .delete_count = self.metrics.getDeleteCount(),
        };
    }

    pub fn compact(self: *Self) !void {
        const db = self.db orelse return StorageError.DatabaseNotInitialized;

        var err_str: ?Data = null;
        defer if (err_str) |e| e.deinit();

        try db.deleteFilesInRange(self.vectors_cf, "", "~", &err_str);
    }

    pub fn putIndexState(
        self: *Self,
        collection: []const u8,
        state_key: []const u8,
        state_data: []const u8,
    ) !void {
        const db = self.db orelse return StorageError.DatabaseNotInitialized;

        const key = try std.fmt.allocPrint(self.allocator, "{s}:{s}", .{ collection, state_key });
        defer self.allocator.free(key);

        var err_str: ?Data = null;
        defer if (err_str) |e| e.deinit();

        try db.put(self.index_state_cf, key, state_data, &err_str);
    }

    pub fn getIndexState(
        self: *Self,
        collection: []const u8,
        state_key: []const u8,
    ) !?[]u8 {
        const db = self.db orelse return StorageError.DatabaseNotInitialized;

        const key = try std.fmt.allocPrint(self.allocator, "{s}:{s}", .{ collection, state_key });
        defer self.allocator.free(key);

        var err_str: ?Data = null;
        defer if (err_str) |e| e.deinit();

        const result = try db.get(self.index_state_cf, key, &err_str);
        if (result == null) return null;

        defer result.?.deinit();

        return try self.allocator.dupe(u8, result.?.data);
    }

    pub fn listPersistedCollections(self: *Self) ![][]const u8 {
        const db = self.db orelse return StorageError.DatabaseNotInitialized;

        var collections = std.StringHashMap(void).init(self.allocator);
        defer collections.deinit();

        var iter = db.iterator(self.index_state_cf, .forward, "");
        defer iter.deinit();

        var err_str: ?Data = null;
        defer if (err_str) |e| e.deinit();

        while (try iter.nextKey(&err_str)) |key| {
            const key_str = key.data;

            if (std.mem.indexOf(u8, key_str, ":config")) |colon_pos| {
                const collection_name = key_str[0..colon_pos];

                const suffix = key_str[colon_pos..];
                if (std.mem.eql(u8, suffix, ":config")) {
                    try collections.put(try self.allocator.dupe(u8, collection_name), {});
                }
            }
        }

        const result = try self.allocator.alloc([]const u8, collections.count());
        var map_iter = collections.keyIterator();
        var i: usize = 0;
        while (map_iter.next()) |name_ptr| {
            result[i] = name_ptr.*;
            i += 1;
        }

        var clear_iter = collections.keyIterator();
        while (clear_iter.next()) |_| {}

        return result;
    }
};

pub const StorageStats = struct {
    estimated_vectors: u64,
    total_size_bytes: u64,
    cache_hit_rate: f64,
    insert_count: u64,
    delete_count: u64,
};
