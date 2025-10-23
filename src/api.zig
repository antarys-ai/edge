const std = @import("std");
const httpz = @import("httpz");
const AntarysDB = @import("vectordb.zig").AntarysDB;
const CollectionConfig = @import("vectordb.zig").CollectionConfig;
const ArrayList = std.ArrayList;
const Allocator = std.mem.Allocator;
const Atomic = std.atomic.Value;
const Thread = std.Thread;

pub const ServerConfig = struct {
    port: u16 = 8080,
    data_dir: []const u8 = "./data",
    cache_size: usize = 10000,
    threads: ?usize = null,
};

const Metrics = struct {
    request_count: Atomic(u64),
    error_count: Atomic(u64),
    active_connections: Atomic(u64),

    fn init() Metrics {
        return .{
            .request_count = Atomic(u64).init(0),
            .error_count = Atomic(u64).init(0),
            .active_connections = Atomic(u64).init(0),
        };
    }
};

const BufferPool = struct {
    buffers: ArrayList([]u8),
    available: ArrayList(*[]u8),
    lock: Thread.Mutex,
    allocator: Allocator,
    buffer_size: usize,

    fn init(allocator: Allocator, count: usize, size: usize) !BufferPool {
        var pool = BufferPool{
            .buffers = try ArrayList([]u8).initCapacity(allocator, count),
            .available = try ArrayList(*[]u8).initCapacity(allocator, count),
            .lock = Thread.Mutex{},
            .allocator = allocator,
            .buffer_size = size,
        };

        for (0..count) |_| {
            const buf = try allocator.alloc(u8, size);
            try pool.buffers.append(allocator, buf);
            try pool.available.append(allocator, &pool.buffers.items[pool.buffers.items.len - 1]);
        }

        return pool;
    }

    fn deinit(self: *BufferPool) void {
        for (self.buffers.items) |buf| {
            self.allocator.free(buf);
        }
        self.buffers.deinit(self.allocator);
        self.available.deinit(self.allocator);
    }

    fn acquire(self: *BufferPool) ![]u8 {
        self.lock.lock();
        defer self.lock.unlock();

        if (self.available.items.len > 0) {
            const buf_ptr = self.available.pop();
            return buf_ptr.*;
        }

        return try self.allocator.alloc(u8, self.buffer_size);
    }

    fn release(self: *BufferPool, buf: []u8) void {
        if (buf.len != self.buffer_size) {
            self.allocator.free(buf);
            return;
        }

        self.lock.lock();
        defer self.lock.unlock();

        for (self.buffers.items) |*pool_buf| {
            if (pool_buf.ptr == buf.ptr) {
                self.available.append(pool_buf) catch {};
                return;
            }
        }

        self.allocator.free(buf);
    }
};

const VectorPool = struct {
    vectors: ArrayList(ArrayList(f32)),
    available: ArrayList(*ArrayList(f32)),
    lock: Thread.Mutex,
    allocator: Allocator,

    fn init(allocator: Allocator, count: usize) !VectorPool {
        var pool = VectorPool{
            .vectors = try ArrayList(ArrayList(f32)).initCapacity(allocator, count),
            .available = try ArrayList(*ArrayList(f32)).initCapacity(allocator, count),
            .lock = Thread.Mutex{},
            .allocator = allocator,
        };

        for (0..count) |_| {
            const vec = try ArrayList(f32).initCapacity(allocator, 1536);
            try pool.vectors.append(allocator, vec);
            try pool.available.append(allocator, &pool.vectors.items[pool.vectors.items.len - 1]);
        }

        return pool;
    }

    fn deinit(self: *VectorPool) void {
        for (self.vectors.items) |*vec| {
            vec.deinit(self.allocator);
        }
        self.vectors.deinit(self.allocator);
        self.available.deinit(self.allocator);
    }

    fn acquire(self: *VectorPool) !ArrayList(f32) {
        self.lock.lock();
        defer self.lock.unlock();

        if (self.available.items.len > 0) {
            const vec_ptr = self.available.pop();

            if (vec_ptr) |ptr| {
                ptr.clearRetainingCapacity();
                return ptr.*;
            }
        }

        return try ArrayList(f32).initCapacity(self.allocator, 1536);
    }

    fn release(self: *VectorPool, vec: ArrayList(f32)) void {
        self.lock.lock();
        defer self.lock.unlock();

        for (self.vectors.items) |*pool_vec| {
            if (pool_vec.items.ptr == vec.items.ptr) {
                self.available.append(self.allocator, pool_vec) catch {};
                return;
            }
        }

        var mutable_vec = vec;
        mutable_vec.deinit(self.allocator);
    }
};

const Handler = struct {
    allocator: Allocator,
    config: ServerConfig,
    db: AntarysDB,
    metrics: Metrics,
    start_time: i64,
    is_ready: Atomic(bool),
    buffer_pool: BufferPool,
    vector_pool: VectorPool,

    fn handleHealth(self: *Handler, req: *httpz.Request, res: *httpz.Response) !void {
        _ = req;
        _ = self.metrics.request_count.fetchAdd(1, .monotonic);

        res.status = 200;
        return res.json(.{ .status = "ok" }, .{});
    }

    fn handleInfo(self: *Handler, req: *httpz.Request, res: *httpz.Response) !void {
        _ = req;

        const now = std.time.milliTimestamp();
        const uptime_seconds = @divFloor(now - self.start_time, 1000);

        res.status = 200;
        return res.json(.{
            .uptime_seconds = uptime_seconds,
            .requests_served = self.metrics.request_count.load(.monotonic),
            .errors = self.metrics.error_count.load(.monotonic),
            .active_connections = self.metrics.active_connections.load(.monotonic),
        }, .{});
    }

    fn handleUpsert(self: *Handler, req: *httpz.Request, res: *httpz.Response) !void {
        std.debug.print("[API] <<<< UPSERT request received >>>>\n", .{});

        const body = req.body();

        if (body == null or body.?.len == 0) {
            std.debug.print("[API] Error: Empty request body\n", .{});
            res.status = 400;
            return res.json(.{ .@"error" = "Empty request body" }, .{});
        }

        std.debug.print("[API] Body length: {d} bytes\n", .{body.?.len});
        std.debug.print("[API] Body preview: {s}\n", .{body.?[0..@min(200, body.?.len)]});

        var parsed = std.json.parseFromSlice(std.json.Value, self.allocator, body.?, .{}) catch |err| {
            std.debug.print("[API] JSON parse error: {}\n", .{err});
            res.status = 400;
            return res.json(.{ .@"error" = "Invalid JSON", .details = @errorName(err) }, .{});
        };
        defer parsed.deinit();

        const root = parsed.value.object;

        if (root.get("vectors")) |vectors_value| {
            std.debug.print("[API] Batch upsert detected ({d} vectors)\n", .{vectors_value.array.items.len});
            const collection = if (root.get("collection")) |c| c.string else "default";
            return try self.handleBatchUpsertInternal(vectors_value, collection, res);
        }

        std.debug.print("[API] Single upsert detected\n", .{});

        const id = root.get("id") orelse {
            std.debug.print("[API] Error: Missing 'id' field\n", .{});
            res.status = 400;
            return res.json(.{ .@"error" = "Missing 'id' field" }, .{});
        };

        const vector_data = root.get("vector") orelse root.get("values") orelse {
            std.debug.print("[API] Error: Missing vector data\n", .{});
            res.status = 400;
            return res.json(.{ .@"error" = "Missing vector data" }, .{});
        };

        const collection = if (root.get("collection")) |c| c.string else "default";

        var vec_list = try self.vector_pool.acquire();
        defer self.vector_pool.release(vec_list);

        try extractVector(self.allocator, vector_data, &vec_list);

        std.debug.print("[API] Inserting vector '{s}' with {d} dimensions to collection '{s}'\n", .{ id.string, vec_list.items.len, collection });

        try self.ensureCollection(collection, vec_list.items.len);

        self.db.insert(collection, id.string, vec_list.items) catch |err| {
            std.debug.print("[API] Insert error for id '{s}': {}\n", .{ id.string, err });
            res.status = 500;
            return res.json(.{ .@"error" = "Insert failed", .details = @errorName(err) }, .{});
        };

        std.debug.print("[API] Insert successful for id '{s}'\n", .{id.string});

        res.status = 200;
        return res.json(.{
            .success = true,
            .id = id.string,
        }, .{});
    }

    fn ensureCollection(self: *Handler, name: []const u8, dimensions: usize) !void {
        const collections = try self.db.listCollections();
        defer {
            for (collections) |col_name| {
                self.allocator.free(col_name);
            }
            self.allocator.free(collections);
        }

        for (collections) |col_name| {
            if (std.mem.eql(u8, col_name, name)) {
                return;
            }
        }

        std.debug.print("[API] Auto-creating collection '{s}' with {d} dimensions\n", .{ name, dimensions });

        self.db.createCollection(name, .{
            .dimensions = dimensions,
            .metric = .cosine,
            .connectivity = 16,
            .expansion_add = 128,
            .expansion_search = 64,
            .enable_persistence = true,
        }) catch |err| {
            std.debug.print("[API] Auto-create collection error: {}\n", .{err});
            return err;
        };

        self.db.saveCollection(name) catch |err| {
            std.debug.print("[API] Warning: Failed to save collection: {}\n", .{err});
        };

        std.debug.print("[API] Collection '{s}' auto-created and saved\n", .{name});
    }

    fn handleBatchUpsertInternal(self: *Handler, vectors_value: std.json.Value, collection: []const u8, res: *httpz.Response) !void {
        const vectors = vectors_value.array;

        if (vectors.items.len == 0) {
            res.status = 400;
            return res.json(.{ .@"error" = "No vectors to insert" }, .{});
        }

        std.debug.print("[API] Starting batch upsert of {d} vectors to collection '{s}'\n", .{ vectors.items.len, collection });

        var ids = try ArrayList([]const u8).initCapacity(self.allocator, vectors.items.len);
        defer ids.deinit(self.allocator);

        var vector_list = try ArrayList([]f32).initCapacity(self.allocator, vectors.items.len);
        defer {
            for (vector_list.items) |vec| {
                self.allocator.free(vec);
            }
            vector_list.deinit(self.allocator);
        }

        var first_dim: ?usize = null;

        for (vectors.items) |vec_value| {
            const vec_obj = vec_value.object;

            const id = vec_obj.get("id") orelse continue;
            const vector_data = vec_obj.get("vector") orelse vec_obj.get("values") orelse continue;

            var vec_list = try self.vector_pool.acquire();
            defer self.vector_pool.release(vec_list);

            try extractVector(self.allocator, vector_data, &vec_list);

            if (first_dim == null) {
                first_dim = vec_list.items.len;
            }

            const vector_copy = try self.allocator.dupe(f32, vec_list.items);

            try ids.append(self.allocator, id.string);
            try vector_list.append(self.allocator, vector_copy);
        }

        if (ids.items.len == 0) {
            res.status = 400;
            return res.json(.{ .@"error" = "No valid vectors to insert" }, .{});
        }

        std.debug.print("[API] Parsed {d} valid vectors, calling insertBatch\n", .{ids.items.len});

        try self.ensureCollection(collection, first_dim orelse 1536);

        self.db.insertBatch(collection, ids.items, vector_list.items) catch |err| {
            std.debug.print("[API] Batch insert error: {}\n", .{err});
            res.status = 500;
            return res.json(.{ .@"error" = "Batch insert failed", .details = @errorName(err) }, .{});
        };

        std.debug.print("[API] Batch insert successful\n", .{});

        const count = self.db.count(collection) catch 0;
        std.debug.print("[API] Collection '{s}' now has {d} vectors\n", .{ collection, count });

        res.status = 200;
        return res.json(.{
            .success = true,
            .count = ids.items.len,
        }, .{});
    }

    fn handleQuery(self: *Handler, req: *httpz.Request, res: *httpz.Response) !void {
        const body = req.body();

        if (body == null) {
            res.status = 400;
            return res.json(.{ .@"error" = "Empty request body" }, .{});
        }

        var parsed = try std.json.parseFromSlice(std.json.Value, self.allocator, body.?, .{});
        defer parsed.deinit();

        const root = parsed.value.object;

        const vector_data = root.get("vector") orelse root.get("values") orelse {
            res.status = 400;
            return res.json(.{ .@"error" = "Missing vector data" }, .{});
        };

        const collection = if (root.get("collection")) |c| c.string else "default";
        const top_k = if (root.get("top_k")) |k| blk: {
            break :blk switch (k) {
                .integer => |i| @as(usize, @intCast(i)),
                .float => |f| @as(usize, @intFromFloat(f)),
                else => 10,
            };
        } else 10;
        const include_metadata = if (root.get("include_metadata")) |m| m.bool else false;
        const include_vectors = if (root.get("include_vectors")) |v| v.bool else false;

        var vec_list = try self.vector_pool.acquire();
        defer self.vector_pool.release(vec_list);

        try extractVector(self.allocator, vector_data, &vec_list);

        const results = try self.db.search(collection, vec_list.items, top_k, include_vectors);
        defer {
            for (results) |*r| r.deinit(self.allocator);
            self.allocator.free(results);
        }

        var response_array = std.json.Array.init(self.allocator);
        defer response_array.deinit();

        for (results) |result| {
            var result_obj = std.json.ObjectMap.init(self.allocator);

            try result_obj.put("id", .{ .string = result.id });
            try result_obj.put("score", .{ .float = result.distance });

            if (include_vectors) {
                if (result.vector) |vec| {
                    var vec_array = std.json.Array.init(self.allocator);
                    for (vec) |val| {
                        try vec_array.append(.{ .float = val });
                    }
                    try result_obj.put("vector", .{ .array = vec_array });
                }
            }

            if (include_metadata) {
                const vec_data = try self.db.get(collection, result.id);
                if (vec_data) |_| {
                    defer if (vec_data) |v| self.allocator.free(v);
                    const metadata = std.json.ObjectMap.init(self.allocator);
                    try result_obj.put("metadata", .{ .object = metadata });
                }
            }

            try response_array.append(.{ .object = result_obj });
        }

        res.status = 200;
        return res.json(.{
            .results = response_array.items,
            .count = results.len,
            .matches = response_array.items,
        }, .{});
    }

    fn handleBatchQuery(self: *Handler, req: *httpz.Request, res: *httpz.Response) !void {
        const body = req.body();

        if (body == null) {
            res.status = 400;
            return res.json(.{ .@"error" = "Empty request body" }, .{});
        }

        var parsed = try std.json.parseFromSlice(std.json.Value, self.allocator, body.?, .{});
        defer parsed.deinit();

        const root = parsed.value.object;

        const queries_value = root.get("queries") orelse {
            res.status = 400;
            return res.json(.{ .@"error" = "Missing queries" }, .{});
        };

        const queries = queries_value.array;
        const collection = if (root.get("collection")) |c| c.string else "default";
        const include_metadata = if (root.get("include_metadata")) |m| m.bool else false;
        const include_vectors = if (root.get("include_vectors")) |v| v.bool else false;

        var response_array = std.json.Array.init(self.allocator);
        defer response_array.deinit();

        for (queries.items, 0..) |query_value, i| {
            const query_obj = query_value.object;

            const vector_data = query_obj.get("vector") orelse query_obj.get("values") orelse continue;

            const top_k = if (query_obj.get("top_k")) |k| blk: {
                break :blk switch (k) {
                    .integer => |j| @as(usize, @intCast(j)),
                    .float => |f| @as(usize, @intFromFloat(f)),
                    else => 10,
                };
            } else 10;

            var vec_list = try self.vector_pool.acquire();
            defer self.vector_pool.release(vec_list);

            try extractVector(self.allocator, vector_data, &vec_list);

            const results = try self.db.search(collection, vec_list.items, top_k, include_vectors);
            defer {
                for (results) |*r| r.deinit(self.allocator);
                self.allocator.free(results);
            }

            var query_result = std.json.ObjectMap.init(self.allocator);

            const query_id = try std.fmt.allocPrint(self.allocator, "query_{d}", .{i});
            try query_result.put("query_id", .{ .string = query_id });
            try query_result.put("count", .{ .integer = @intCast(results.len) });

            var result_array = std.json.Array.init(self.allocator);

            for (results) |result| {
                var result_obj = std.json.ObjectMap.init(self.allocator);

                try result_obj.put("id", .{ .string = result.id });
                try result_obj.put("score", .{ .float = result.distance });

                if (include_vectors) {
                    if (result.vector) |vec| {
                        var vec_array = std.json.Array.init(self.allocator);
                        for (vec) |val| {
                            try vec_array.append(.{ .float = val });
                        }
                        try result_obj.put("vector", .{ .array = vec_array });
                    }
                }

                if (include_metadata) {
                    const metadata = std.json.ObjectMap.init(self.allocator);
                    try result_obj.put("metadata", .{ .object = metadata });
                }

                try result_array.append(.{ .object = result_obj });
            }

            try query_result.put("results", .{ .array = result_array });
            try response_array.append(.{ .object = query_result });
        }

        res.status = 200;
        return res.json(.{ .results = response_array.items }, .{});
    }

    fn handleDelete(self: *Handler, req: *httpz.Request, res: *httpz.Response) !void {
        const body = req.body();

        if (body == null) {
            res.status = 400;
            return res.json(.{ .@"error" = "Empty request body" }, .{});
        }

        var parsed = try std.json.parseFromSlice(std.json.Value, self.allocator, body.?, .{});
        defer parsed.deinit();

        const root = parsed.value.object;

        const ids_value = root.get("ids") orelse {
            res.status = 400;
            return res.json(.{ .@"error" = "Missing 'ids' field" }, .{});
        };

        const ids = ids_value.array;
        const collection = if (root.get("collection")) |c| c.string else "default";

        var deleted = try ArrayList([]const u8).initCapacity(self.allocator, ids.items.len);
        defer deleted.deinit(self.allocator);

        var failed = try ArrayList([]const u8).initCapacity(self.allocator, ids.items.len);
        defer failed.deinit(self.allocator);

        for (ids.items) |id_value| {
            const id = id_value.string;
            if (self.db.delete(collection, id)) {
                try deleted.append(self.allocator, id);
            } else |_| {
                try failed.append(self.allocator, id);
            }
        }

        res.status = 200;
        return res.json(.{
            .deleted = deleted.items,
            .failed = failed.items,
        }, .{});
    }

    fn handleGetVector(self: *Handler, req: *httpz.Request, res: *httpz.Response) !void {
        const id = req.param("id") orelse {
            res.status = 400;
            return res.json(.{ .@"error" = "Missing vector ID" }, .{});
        };

        const query_params = try req.query();
        const collection = query_params.get("collection") orelse "default";

        const vec = try self.db.get(collection, id);

        if (vec == null) {
            res.status = 404;
            return res.json(.{ .@"error" = "Vector not found" }, .{});
        }

        defer if (vec) |v| self.allocator.free(v);

        res.status = 200;
        return res.json(.{
            .id = id,
            .vector = vec.?,
            .collection = collection,
        }, .{});
    }

    fn handleCreateCollection(self: *Handler, req: *httpz.Request, res: *httpz.Response) !void {
        std.debug.print("[API] <<<< CREATE COLLECTION request received >>>>\n", .{});

        const body = req.body();

        if (body == null) {
            std.debug.print("[API] Error: Empty request body\n", .{});
            res.status = 400;
            return res.json(.{ .@"error" = "Empty request body" }, .{});
        }

        std.debug.print("[API] Body length: {d} bytes\n", .{body.?.len});
        std.debug.print("[API] Body: {s}\n", .{body.?});

        var parsed = std.json.parseFromSlice(std.json.Value, self.allocator, body.?, .{}) catch |err| {
            std.debug.print("[API] JSON parse error: {}\n", .{err});
            res.status = 400;
            return res.json(.{ .@"error" = "Invalid JSON", .details = @errorName(err) }, .{});
        };
        defer parsed.deinit();

        const root = parsed.value.object;

        const name = root.get("name") orelse {
            std.debug.print("[API] Error: Missing collection name\n", .{});
            res.status = 400;
            return res.json(.{ .@"error" = "Collection name required" }, .{});
        };

        const dimensions = if (root.get("dimensions")) |d| blk: {
            break :blk switch (d) {
                .integer => |i| @as(usize, @intCast(i)),
                .float => |f| @as(usize, @intFromFloat(f)),
                else => 1536,
            };
        } else 1536;

        const m = if (root.get("m")) |m_val| blk: {
            break :blk switch (m_val) {
                .integer => |i| @as(usize, @intCast(i)),
                .float => |f| @as(usize, @intFromFloat(f)),
                else => 16,
            };
        } else 16;

        const ef_construction = if (root.get("ef_construction")) |ef| blk: {
            break :blk switch (ef) {
                .integer => |i| @as(usize, @intCast(i)),
                .float => |f| @as(usize, @intFromFloat(f)),
                else => 100,
            };
        } else 100;

        std.debug.print("[API] Creating collection '{s}' with {d} dimensions, m={d}, ef_construction={d}\n", .{ name.string, dimensions, m, ef_construction });

        self.db.createCollection(name.string, .{
            .dimensions = dimensions,
            .metric = .cosine,
            .connectivity = m,
            .expansion_add = ef_construction,
            .expansion_search = 64,
            .enable_persistence = true,
        }) catch |err| {
            std.debug.print("[API] Create collection error: {}\n", .{err});

            if (err == error.CollectionAlreadyExists) {
                std.debug.print("[API] Collection already exists, returning existing\n", .{});
                res.status = 200;
                return res.json(.{
                    .success = true,
                    .name = name.string,
                    .dimensions = dimensions,
                    .already_exists = true,
                }, .{});
            }

            res.status = 500;
            return res.json(.{ .@"error" = "Failed to create collection", .details = @errorName(err) }, .{});
        };

        self.db.saveCollection(name.string) catch |err| {
            std.debug.print("[API] Warning: Failed to save collection after creation: {}\n", .{err});
        };

        std.debug.print("[API] Collection '{s}' created and saved successfully\n", .{name.string});

        res.status = 201;
        return res.json(.{
            .success = true,
            .name = name.string,
            .dimensions = dimensions,
        }, .{});
    }

    fn handleListCollections(self: *Handler, req: *httpz.Request, res: *httpz.Response) !void {
        _ = req;

        const collections = try self.db.listCollections();
        defer {
            for (collections) |name| {
                self.allocator.free(name);
            }
            self.allocator.free(collections);
        }

        var collection_array = std.json.Array.init(self.allocator);
        defer collection_array.deinit();

        for (collections) |name| {
            const count = try self.db.count(name);

            var info = std.json.ObjectMap.init(self.allocator);
            try info.put("name", .{ .string = name });
            try info.put("count", .{ .integer = @intCast(count) });

            try collection_array.append(.{ .object = info });
        }

        res.status = 200;
        return res.json(.{
            .collections = collection_array.items,
            .count = collections.len,
        }, .{});
    }

    fn handleDeleteCollection(self: *Handler, req: *httpz.Request, res: *httpz.Response) !void {
        const name = req.param("name") orelse {
            res.status = 400;
            return res.json(.{ .@"error" = "Collection name required" }, .{});
        };

        try self.db.deleteCollection(name);

        res.status = 200;
        return res.json(.{
            .success = true,
            .name = name,
        }, .{});
    }

    fn handleCompact(self: *Handler, req: *httpz.Request, res: *httpz.Response) !void {
        _ = req;

        try self.db.compact();

        res.status = 200;
        return res.json(.{ .success = true }, .{});
    }

    fn handleSaveCollection(self: *Handler, req: *httpz.Request, res: *httpz.Response) !void {
        const name = req.param("name") orelse {
            res.status = 400;
            return res.json(.{ .@"error" = "Collection name required" }, .{});
        };

        std.debug.print("[API] Manual save requested for collection '{s}'\n", .{name});
        try self.db.saveCollection(name);
        std.debug.print("[API] Manual save completed for collection '{s}'\n", .{name});

        res.status = 200;
        return res.json(.{
            .success = true,
            .collection = name,
        }, .{});
    }

    fn handleSaveAll(self: *Handler, req: *httpz.Request, res: *httpz.Response) !void {
        _ = req;

        std.debug.print("[API] Manual saveAll requested\n", .{});
        try self.db.saveAll();
        std.debug.print("[API] Manual saveAll completed\n", .{});

        res.status = 200;
        return res.json(.{
            .success = true,
            .message = "All collections saved",
        }, .{});
    }
};

fn extractVector(allocator: Allocator, value: std.json.Value, list: *ArrayList(f32)) !void {
    const array = value.array;
    try list.ensureTotalCapacity(allocator, array.items.len);

    for (array.items) |item| {
        const val: f32 = switch (item) {
            .float => |f| @floatCast(f),
            .integer => |i| @floatFromInt(i),
            .string => |s| std.fmt.parseFloat(f32, s) catch continue,
            else => continue,
        };
        try list.append(allocator, val);
    }
}

var global_shutdown = Atomic(bool).init(false);
var global_handler: ?*Handler = null;

pub fn start(allocator: Allocator, config: ServerConfig) !void {
    std.debug.print("[SERVER] Initializing database...\n", .{});
    var db = try AntarysDB.init(allocator, .{
        .storage_path = config.data_dir,
        .enable_cache = true,
        .cache_size = @intCast(config.cache_size / 2),
    });
    defer {
        std.debug.print("[SERVER] Saving all collections before shutdown...\n", .{});
        db.saveAll() catch |err| {
            std.debug.print("[SERVER] Error saving collections: {}\n", .{err});
        };
        db.deinit();
    }
    std.debug.print("[SERVER] Database initialized\n", .{});

    var buffer_pool = try BufferPool.init(allocator, 1000, 64 * 1024);
    defer buffer_pool.deinit();

    var vector_pool = try VectorPool.init(allocator, 1000);
    defer vector_pool.deinit();

    var handler = Handler{
        .allocator = allocator,
        .config = config,
        .db = db,
        .metrics = Metrics.init(),
        .start_time = std.time.milliTimestamp(),
        .is_ready = Atomic(bool).init(false),
        .buffer_pool = buffer_pool,
        .vector_pool = vector_pool,
    };

    global_handler = &handler;
    defer global_handler = null;

    const cpu_count = std.Thread.getCpuCount() catch 8;

    var server = try httpz.Server(*Handler).init(allocator, .{
        .port = config.port,
        .address = "0.0.0.0",

        .workers = .{
            .count = @intCast(cpu_count),
            .max_conn = 8192,
            .min_conn = 32,
            .large_buffer_count = 512,
            .large_buffer_size = 256 * 1024,
            .retain_allocated_bytes = 8192,
        },

        .thread_pool = .{
            .count = @intCast(cpu_count * 4),
            .backlog = 1000,
            .buffer_size = 128 * 1024,
        },

        .request = .{
            .max_body_size = 100 * 1024 * 1024,
            .buffer_size = 32 * 1024,
            .max_header_count = 64,
            .max_param_count = 32,
            .max_query_count = 64,
            .max_form_count = 64,
            .max_multiform_count = 32,
        },

        .response = .{
            .max_header_count = 64,
        },

        .timeout = .{
            .request = 60_000,
            .keepalive = 120_000,
            .request_count = 5000,
        },
    }, &handler);

    defer server.deinit();
    defer server.stop();

    var router = try server.router(.{});

    std.debug.print("[SERVER] Registering routes...\n", .{});
    router.get("/health", Handler.handleHealth, .{});
    router.get("/info", Handler.handleInfo, .{});

    router.post("/vectors/upsert", Handler.handleUpsert, .{});
    router.post("/vectors/query", Handler.handleQuery, .{});
    router.post("/vectors/delete", Handler.handleDelete, .{});
    router.post("/vectors/batch_query", Handler.handleBatchQuery, .{});
    router.get("/vectors/:id", Handler.handleGetVector, .{});

    router.post("/collections", Handler.handleCreateCollection, .{});
    router.get("/collections", Handler.handleListCollections, .{});
    router.delete("/collections/:name", Handler.handleDeleteCollection, .{});

    router.post("/admin/compact", Handler.handleCompact, .{});
    router.post("/admin/save/:name", Handler.handleSaveCollection, .{});
    router.post("/admin/saveall", Handler.handleSaveAll, .{});
    std.debug.print("[SERVER] Routes registered\n", .{});

    handler.is_ready.store(true, .seq_cst);

    std.debug.print("✓ Server ready on http://0.0.0.0:{d}\n", .{config.port});
    std.debug.print("✓ Press Ctrl+C to shutdown gracefully\n\n", .{});
    std.debug.print("[SERVER] Listening for connections...\n", .{});

    try server.listen();
}

pub fn requestShutdown() void {
    global_shutdown.store(true, .release);

    if (global_handler) |handler| {
        handler.is_ready.store(false, .release);

        const max_wait_seconds: u64 = 30;
        const ts = std.time.milliTimestamp();

        while (handler.metrics.active_connections.load(.acquire) > 0) {
            const elapsed = std.time.milliTimestamp() - ts;
            if (elapsed > max_wait_seconds * 1000) {
                std.debug.print("Force shutdown after {d}s with {d} active connections\n", .{
                    max_wait_seconds,
                    handler.metrics.active_connections.load(.acquire),
                });
                break;
            }
            Thread.sleep(100 * std.time.ns_per_ms);
        }

        std.debug.print("All connections closed. Shutting down...\n", .{});

        std.debug.print("[SHUTDOWN] Saving all collections before exit...\n", .{});
        handler.db.saveAll() catch |err| {
            std.debug.print("[SHUTDOWN] ERROR: Failed to save collections: {}\n", .{err});
        };

        std.debug.print("[SHUTDOWN] Save completed\n", .{});

        std.posix.exit(0);
    }
}
