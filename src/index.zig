const std = @import("std");
const usearch = @import("usearch.zig");
const search = @import("search.zig");

fn detectCPUCores() usize {
    const cores = std.Thread.getCpuCount() catch 4;
    return @min(cores, 8);
}

pub const IndexConfig = struct {
    dimensions: usize,
    metric: usearch.Metric = .cosine,
    quantization: usearch.Quantization = .f32,
    connectivity: usize = 16,
    expansion_add: usize = 128,
    expansion_search: usize = 64,
    initial_capacity: usize = 100_000,
    threads_add: ?usize = null,
    threads_search: ?usize = null,

    pub fn getThreadsAdd(self: IndexConfig) usize {
        if (self.threads_add) |t| {
            if (t == 0) return detectCPUCores();
            return t;
        }
        return detectCPUCores();
    }

    pub fn getThreadsSearch(self: IndexConfig) usize {
        if (self.threads_search) |t| {
            if (t == 0) return @max(1, detectCPUCores() / 2);
            return t;
        }
        return @max(1, detectCPUCores() / 2);
    }
};

pub const IndexError = error{
    InitFailed,
    AddFailed,
    RemoveFailed,
    SaveFailed,
    LoadFailed,
    InvalidConfig,
    DimensionMismatch,
    OutOfMemory,
};

pub const VectorIndex = struct {
    index: usearch.Index,
    id_map: search.IdMap,
    config: IndexConfig,
    allocator: std.mem.Allocator,
    mu: std.Thread.Mutex,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, config: IndexConfig) IndexError!VectorIndex {
        if (config.dimensions == 0) return IndexError.InvalidConfig;

        const uconfig = usearch.IndexConfig{
            .dimensions = config.dimensions,
            .metric = config.metric,
            .quantization = config.quantization,
            .connectivity = config.connectivity,
            .expansion_add = config.expansion_add,
            .expansion_search = config.expansion_search,
            .initial_capacity = config.initial_capacity,
        };

        var idx = usearch.Index.init(allocator, uconfig) catch {
            return IndexError.InitFailed;
        };

        const threads_add = config.getThreadsAdd();
        const threads_search = config.getThreadsSearch();

        std.debug.print("[INDEX] Configuring multi-threading: add={d} threads, search={d} threads\n", .{ threads_add, threads_search });

        idx.setThreadsAdd(threads_add) catch |err| {
            std.debug.print("[INDEX] Warning: Failed to set threads_add: {}\n", .{err});
        };

        idx.setThreadsSearch(threads_search) catch |err| {
            std.debug.print("[INDEX] Warning: Failed to set threads_search: {}\n", .{err});
        };

        return .{
            .index = idx,
            .id_map = search.IdMap.init(allocator),
            .config = config,
            .allocator = allocator,
            .mu = .{},
        };
    }

    pub fn deinit(self: *VectorIndex) void {
        self.index.deinit();
        self.id_map.deinit();
    }

    pub fn add(self: *VectorIndex, id: []const u8, vector: []const f32) IndexError!void {
        if (vector.len != self.config.dimensions) {
            return IndexError.DimensionMismatch;
        }

        self.mu.lock();
        const key = self.id_map.getOrCreate(id) catch {
            self.mu.unlock();
            return IndexError.OutOfMemory;
        };
        self.mu.unlock();

        self.index.add(key, vector) catch {
            return IndexError.AddFailed;
        };
    }

    pub fn addBatch(self: *VectorIndex, ids: []const []const u8, vectors: []const []const f32) IndexError!void {
        if (ids.len != vectors.len) return IndexError.InvalidConfig;
        if (ids.len == 0) return;

        for (vectors) |vec| {
            if (vec.len != self.config.dimensions) {
                return IndexError.DimensionMismatch;
            }
        }

        std.debug.print("[INDEX] Starting batch add of {d} vectors...\n", .{ids.len});

        const keys = self.allocator.alloc(u64, ids.len) catch {
            return IndexError.OutOfMemory;
        };
        defer self.allocator.free(keys);

        self.mu.lock();
        for (ids, 0..) |id, i| {
            keys[i] = self.id_map.getOrCreate(id) catch {
                self.mu.unlock();
                return IndexError.OutOfMemory;
            };
        }
        self.mu.unlock();

        for (keys, vectors, 0..) |key, vec, i| {
            self.index.add(key, vec) catch {
                return IndexError.AddFailed;
            };

            if ((i + 1) % 1000 == 0) {
                std.debug.print("[INDEX] Added {d}/{d} vectors to index\n", .{ i + 1, ids.len });
            }
        }

        std.debug.print("[INDEX] Completed batch add\n", .{});
    }

    pub fn remove(self: *VectorIndex, id: []const u8) IndexError!void {
        self.mu.lock();
        defer self.mu.unlock();

        const key = self.id_map.getKey(id) orelse return;

        self.index.remove(key) catch {
            return IndexError.RemoveFailed;
        };

        self.id_map.remove(id) catch {};
    }

    pub fn get(self: *VectorIndex, id: []const u8) IndexError!?[]f32 {
        self.mu.lock();
        const key = self.id_map.getKey(id) orelse {
            self.mu.unlock();
            return null;
        };
        self.mu.unlock();

        return self.index.get(key, 1) catch null;
    }

    pub fn contains(self: *VectorIndex, id: []const u8) bool {
        self.mu.lock();
        defer self.mu.unlock();

        const key = self.id_map.getKey(id) orelse return false;
        return (self.index.contains(key) catch false);
    }

    pub fn len(self: *VectorIndex) usize {
        return self.index.len() catch 0;
    }

    pub fn capacity(self: *VectorIndex) usize {
        return self.index.capacity() catch 0;
    }

    pub fn memoryUsage(self: *VectorIndex) usize {
        return self.index.memoryUsage() catch 0;
    }

    pub fn saveIndex(self: *VectorIndex, path: []const u8) IndexError!void {
        self.mu.lock();
        defer self.mu.unlock();

        self.index.save(path) catch {
            return IndexError.SaveFailed;
        };
    }

    pub fn loadIndex(self: *VectorIndex, path: []const u8) IndexError!void {
        self.mu.lock();
        defer self.mu.unlock();

        self.index.load(path) catch {
            return IndexError.LoadFailed;
        };
    }

    pub fn reserve(self: *VectorIndex, cap: usize) IndexError!void {
        self.index.reserve(cap) catch {
            return IndexError.InitFailed;
        };
    }

    pub fn setExpansionAdd(self: *VectorIndex, expansion: usize) IndexError!void {
        self.index.setExpansionAdd(expansion) catch {
            return IndexError.InitFailed;
        };
    }

    pub fn setExpansionSearch(self: *VectorIndex, expansion: usize) IndexError!void {
        self.index.setExpansionSearch(expansion) catch {
            return IndexError.InitFailed;
        };
    }
};
