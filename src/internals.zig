const std = @import("std");

pub const CacheEntry = struct {
    key: []const u8,
    value: []const u8,
    timestamp: i64,

    pub fn deinit(self: *CacheEntry, allocator: std.mem.Allocator) void {
        allocator.free(self.key);
        allocator.free(self.value);
    }
};

pub const LRUCache = struct {
    map: std.StringHashMap(CacheEntry),
    capacity: usize,
    allocator: std.mem.Allocator,
    mu: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator, capacity: usize) LRUCache {
        return .{
            .map = std.StringHashMap(CacheEntry).init(allocator),
            .capacity = capacity,
            .allocator = allocator,
            .mu = .{},
        };
    }

    pub fn deinit(self: *LRUCache) void {
        var it = self.map.valueIterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key);
            self.allocator.free(entry.value);
        }
        self.map.deinit();
    }

    pub fn get(self: *LRUCache, key: []const u8) ?[]const u8 {
        self.mu.lock();
        defer self.mu.unlock();

        if (self.map.getPtr(key)) |entry| {
            entry.timestamp = std.time.timestamp();
            return entry.value;
        }
        return null;
    }

    pub fn put(self: *LRUCache, key: []const u8, value: []const u8) !void {
        self.mu.lock();
        defer self.mu.unlock();

        if (self.map.count() >= self.capacity) {
            try self.evict();
        }

        const key_copy = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(key_copy);

        const value_copy = try self.allocator.dupe(u8, value);
        errdefer self.allocator.free(value_copy);

        const result = try self.map.fetchPut(key_copy, .{
            .key = key_copy,
            .value = value_copy,
            .timestamp = std.time.timestamp(),
        });

        if (result) |old| {
            self.allocator.free(old.key);
            self.allocator.free(old.value.value);
        }
    }

    pub fn remove(self: *LRUCache, key: []const u8) void {
        self.mu.lock();
        defer self.mu.unlock();

        if (self.map.fetchRemove(key)) |entry| {
            self.allocator.free(entry.value.key);
            self.allocator.free(entry.value.value);
        }
    }

    fn evict(self: *LRUCache) !void {
        var oldest_key: ?[]const u8 = null;
        var oldest_time: i64 = std.math.maxInt(i64);

        var it = self.map.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.timestamp < oldest_time) {
                oldest_time = entry.value_ptr.timestamp;
                oldest_key = entry.key_ptr.*;
            }
        }

        if (oldest_key) |key| {
            if (self.map.fetchRemove(key)) |entry| {
                self.allocator.free(entry.value.key);
                self.allocator.free(entry.value.value);
            }
        }
    }
};

pub fn SearchResultCompare(context: void, a: SearchResultItem, b: SearchResultItem) std.math.Order {
    _ = context;
    return std.math.order(a.distance, b.distance);
}

pub const SearchResultItem = struct {
    key: u64,
    distance: f32,
};

pub const SearchResultQueue = std.PriorityQueue(SearchResultItem, void, SearchResultCompare);

pub const WorkerPool = struct {
    threads: []std.Thread,
    queue: std.ArrayList(Job),
    allocator: std.mem.Allocator,
    mu: std.Thread.Mutex,
    cond: std.Thread.Condition,
    shutdown: std.atomic.Value(bool),

    const Job = struct {
        func: *const fn (*anyopaque) void,
        ctx: *anyopaque,
    };

    pub fn init(allocator: std.mem.Allocator, num_workers: usize) !WorkerPool {
        var pool = WorkerPool{
            .threads = try allocator.alloc(std.Thread, num_workers),
            .queue = try std.ArrayList(Job).initCapacity(allocator, 100),
            .allocator = allocator,
            .mu = .{},
            .cond = .{},
            .shutdown = std.atomic.Value(bool).init(false),
        };

        for (0..num_workers) |i| {
            pool.threads[i] = try std.Thread.spawn(.{}, worker, .{&pool});
        }

        return pool;
    }

    pub fn deinit(self: *WorkerPool) void {
        self.shutdown.store(true, .release);
        self.cond.broadcast();

        for (self.threads) |thread| {
            thread.join();
        }

        self.allocator.free(self.threads);
        self.queue.deinit(self.allocator);
    }

    pub fn submit(self: *WorkerPool, comptime func: fn (*anyopaque) void, ctx: *anyopaque) !void {
        self.mu.lock();
        defer self.mu.unlock();

        try self.queue.append(self.allocator, .{
            .func = func,
            .ctx = ctx,
        });

        self.cond.signal();
    }

    fn worker(self: *WorkerPool) void {
        while (true) {
            self.mu.lock();

            while (self.queue.items.len == 0 and !self.shutdown.load(.acquire)) {
                self.cond.wait(&self.mu);
            }

            if (self.shutdown.load(.acquire) and self.queue.items.len == 0) {
                self.mu.unlock();
                break;
            }

            const job = self.queue.orderedRemove(0);
            self.mu.unlock();

            job.func(job.ctx);
        }
    }
};

pub const Metrics = struct {
    search_count: std.atomic.Value(u64),
    insert_count: std.atomic.Value(u64),
    delete_count: std.atomic.Value(u64),
    cache_hits: std.atomic.Value(u64),
    cache_misses: std.atomic.Value(u64),
    total_latency_ns: std.atomic.Value(u64),

    pub fn init() Metrics {
        return .{
            .search_count = std.atomic.Value(u64).init(0),
            .insert_count = std.atomic.Value(u64).init(0),
            .delete_count = std.atomic.Value(u64).init(0),
            .cache_hits = std.atomic.Value(u64).init(0),
            .cache_misses = std.atomic.Value(u64).init(0),
            .total_latency_ns = std.atomic.Value(u64).init(0),
        };
    }

    pub fn recordSearch(self: *Metrics, latency_ns: u64) void {
        _ = self.search_count.fetchAdd(1, .monotonic);
        _ = self.total_latency_ns.fetchAdd(latency_ns, .monotonic);
    }

    pub fn recordInsert(self: *Metrics) void {
        _ = self.insert_count.fetchAdd(1, .monotonic);
    }

    pub fn recordDelete(self: *Metrics) void {
        _ = self.delete_count.fetchAdd(1, .monotonic);
    }

    pub fn recordCacheHit(self: *Metrics) void {
        _ = self.cache_hits.fetchAdd(1, .monotonic);
    }

    pub fn recordCacheMiss(self: *Metrics) void {
        _ = self.cache_misses.fetchAdd(1, .monotonic);
    }

    pub fn getSearchCount(self: *const Metrics) u64 {
        return self.search_count.load(.monotonic);
    }

    pub fn getInsertCount(self: *const Metrics) u64 {
        return self.insert_count.load(.monotonic);
    }

    pub fn getDeleteCount(self: *const Metrics) u64 {
        return self.delete_count.load(.monotonic);
    }

    pub fn getCacheHitRate(self: *const Metrics) f64 {
        const hits = self.cache_hits.load(.monotonic);
        const misses = self.cache_misses.load(.monotonic);
        const total = hits + misses;

        if (total == 0) return 0.0;
        return @as(f64, @floatFromInt(hits)) / @as(f64, @floatFromInt(total));
    }

    pub fn getAvgLatency(self: *const Metrics) f64 {
        const total = self.total_latency_ns.load(.monotonic);
        const count = self.search_count.load(.monotonic);

        if (count == 0) return 0.0;
        return @as(f64, @floatFromInt(total)) / @as(f64, @floatFromInt(count));
    }
};

pub fn encodeKey(allocator: std.mem.Allocator, collection: []const u8, id: []const u8) ![]u8 {
    return try std.fmt.allocPrint(allocator, "{s}:{s}", .{ collection, id });
}

pub fn decodeKey(key: []const u8) ?struct { collection: []const u8, id: []const u8 } {
    const idx = std.mem.indexOf(u8, key, ":") orelse return null;
    return .{
        .collection = key[0..idx],
        .id = key[idx + 1 ..],
    };
}

pub fn serializeVector(allocator: std.mem.Allocator, vector: []const f32) ![]u8 {
    const size = vector.len * @sizeOf(f32);
    const buf = try allocator.alloc(u8, size);
    const vec_bytes = std.mem.sliceAsBytes(vector);
    @memcpy(buf, vec_bytes);
    return buf;
}

pub fn deserializeVector(data: []const u8, allocator: std.mem.Allocator) ![]f32 {
    if (data.len % @sizeOf(f32) != 0) return error.InvalidData;

    const len = data.len / @sizeOf(f32);
    const vector = try allocator.alloc(f32, len);

    const src = std.mem.bytesAsSlice(f32, data);
    @memcpy(vector, src);

    return vector;
}
