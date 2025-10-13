const std = @import("std");
const usearch = @import("usearch.zig");
const search = @import("search.zig");

pub const IndexConfig = struct {
    dimensions: usize,
    metric: usearch.Metric = .cosine,
    quantization: usearch.Quantization = .f32,
    connectivity: usize = 16,
    expansion_add: usize = 128,
    expansion_search: usize = 64,
    initial_capacity: usize = 100_000,
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

        const idx = usearch.Index.init(allocator, uconfig) catch {
            return IndexError.InitFailed;
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
        defer self.mu.unlock();

        const key = self.id_map.getOrCreate(id) catch {
            return IndexError.OutOfMemory;
        };

        self.index.add(key, vector) catch {
            return IndexError.AddFailed;
        };
    }

    pub fn addBatch(self: *VectorIndex, ids: []const []const u8, vectors: []const []const f32) IndexError!void {
        if (ids.len != vectors.len) return IndexError.InvalidConfig;

        for (ids, vectors) |id, vec| {
            try self.add(id, vec);
        }
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
        defer self.mu.unlock();

        const key = self.id_map.getKey(id) orelse return null;
        return self.index.get(key, 1) catch null;
    }

    pub fn contains(self: *VectorIndex, id: []const u8) bool {
        self.mu.lock();
        defer self.mu.unlock();

        const key = self.id_map.getKey(id) orelse return false;
        return (self.index.contains(key) catch false);
    }

    pub fn len(self: *VectorIndex) usize {
        self.mu.lock();
        defer self.mu.unlock();

        return self.index.len() catch 0;
    }

    pub fn capacity(self: *VectorIndex) usize {
        self.mu.lock();
        defer self.mu.unlock();

        return self.index.capacity() catch 0;
    }

    pub fn memoryUsage(self: *VectorIndex) usize {
        self.mu.lock();
        defer self.mu.unlock();

        return self.index.memoryUsage() catch 0;
    }

    pub fn save(self: *VectorIndex, path: []const u8) IndexError!void {
        self.mu.lock();
        defer self.mu.unlock();

        self.index.save(path) catch {
            return IndexError.SaveFailed;
        };

        const meta_path = try std.fmt.allocPrint(
            self.allocator,
            "{s}.meta",
            .{path},
        );
        defer self.allocator.free(meta_path);

        try self.saveMetadata(meta_path);
    }

    pub fn load(self: *VectorIndex, path: []const u8) IndexError!void {
        self.mu.lock();
        defer self.mu.unlock();

        self.index.load(path) catch {
            return IndexError.LoadFailed;
        };

        const meta_path = try std.fmt.allocPrint(
            self.allocator,
            "{s}.meta",
            .{path},
        );
        defer self.allocator.free(meta_path);

        self.loadMetadata(meta_path) catch {};
    }

    pub fn reserve(self: *VectorIndex, cap: usize) IndexError!void {
        self.mu.lock();
        defer self.mu.unlock();

        self.index.reserve(cap) catch {
            return IndexError.InitFailed;
        };
    }

    pub fn setExpansionAdd(self: *VectorIndex, expansion: usize) IndexError!void {
        self.mu.lock();
        defer self.mu.unlock();

        self.index.setExpansionAdd(expansion) catch {
            return IndexError.InitFailed;
        };
    }

    pub fn setExpansionSearch(self: *VectorIndex, expansion: usize) IndexError!void {
        self.mu.lock();
        defer self.mu.unlock();

        self.index.setExpansionSearch(expansion) catch {
            return IndexError.InitFailed;
        };
    }

    fn saveMetadata(self: *VectorIndex, path: []const u8) !void {
        const file = try std.fs.cwd().createFile(path, .{});
        defer file.close();

        const writer = file.writer();

        try writer.writeInt(usize, self.config.dimensions, .little);
        try writer.writeInt(u8, @intFromEnum(self.config.metric), .little);
        try writer.writeInt(u8, @intFromEnum(self.config.quantization), .little);
        try writer.writeInt(usize, self.config.connectivity, .little);
        try writer.writeInt(usize, self.id_map.next_key, .little);

        try writer.writeInt(usize, self.id_map.id_to_key.count(), .little);

        var it = self.id_map.id_to_key.iterator();
        while (it.next()) |entry| {
            try writer.writeInt(usize, entry.key_ptr.len, .little);
            try writer.writeAll(entry.key_ptr.*);
            try writer.writeInt(u64, entry.value_ptr.*, .little);
        }
    }

    fn loadMetadata(self: *VectorIndex, path: []const u8) !void {
        const file = try std.fs.cwd().openFile(path, .{});
        defer file.close();

        const reader = file.reader();

        _ = try reader.readInt(usize, .little);
        _ = try reader.readInt(u8, .little);
        _ = try reader.readInt(u8, .little);
        _ = try reader.readInt(usize, .little);

        self.id_map.next_key = try reader.readInt(usize, .little);

        const count = try reader.readInt(usize, .little);

        for (0..count) |_| {
            const id_len = try reader.readInt(usize, .little);
            const id = try self.allocator.alloc(u8, id_len);
            errdefer self.allocator.free(id);

            _ = try reader.readAll(id);
            const key = try reader.readInt(u64, .little);

            try self.id_map.id_to_key.put(id, key);
            try self.id_map.key_to_id.put(key, id);
        }
    }
};
