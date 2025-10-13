const std = @import("std");
const lmdbx = @import("lmdbx");

pub const StorageError = error{
    InitFailed,
    OpenFailed,
    CloseFailed,
    TxnFailed,
    GetFailed,
    PutFailed,
    DeleteFailed,
    NotFound,
    OutOfMemory,
    DatabaseFull,
} || lmdbx.Error;

pub const StorageConfig = struct {
    path: []const u8,
    max_dbs: u32 = 128,
    max_readers: u32 = 126,
    map_size: usize = 10 * 1024 * 1024 * 1024,
    sync_mode: SyncMode = .safe,
};

pub const SyncMode = enum {
    safe,
    async_safe,
    async_unsafe,
};

pub const Entry = lmdbx.Cursor.Entry;

pub const Storage = struct {
    env: lmdbx.Environment,
    config: StorageConfig,
    allocator: std.mem.Allocator,
    mu: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator, config: StorageConfig) StorageError!Storage {
        const path_z = try allocator.dupeZ(u8, config.path);
        defer allocator.free(path_z);

        const sys_pagesize = std.heap.pageSize();

        const env = lmdbx.Environment.init(path_z, .{
            .geometry = .{
                .size_now = @intCast(config.map_size),
                .upper_size = @intCast(config.map_size * 2),
                .growth_step = 1024 * 1024 * 256,
                .pagesize = @intCast(sys_pagesize),
            },
            .max_dbs = config.max_dbs,
            .max_readers = config.max_readers,
            .write_map = true,
            .lifo_reclaim = true,
            .no_mem_init = true,
            .no_meta_sync = switch (config.sync_mode) {
                .safe => false,
                .async_safe, .async_unsafe => true,
            },
            .safe_nosync = config.sync_mode == .async_safe,
            .unsafe_nosync = config.sync_mode == .async_unsafe,
        }) catch return StorageError.InitFailed;

        return .{
            .env = env,
            .config = config,
            .allocator = allocator,
            .mu = .{},
        };
    }

    pub fn deinit(self: *Storage) void {
        self.mu.lock();
        defer self.mu.unlock();

        self.env.deinit() catch {};
    }

    pub fn get(self: *Storage, key: []const u8, allocator: std.mem.Allocator) StorageError!?[]u8 {
        self.mu.lock();
        defer self.mu.unlock();

        const txn = self.env.transaction(.{
            .mode = .ReadOnly,
        }) catch return StorageError.TxnFailed;
        defer txn.abort() catch {};

        const db = txn.database(null, .{}) catch return StorageError.OpenFailed;

        const data = db.get(key) catch |err| {
            return switch (err) {
                lmdbx.Error.MDBX_NOTFOUND => null,
                else => StorageError.GetFailed,
            };
        } orelse return null;

        return try allocator.dupe(u8, data);
    }

    pub fn put(self: *Storage, key: []const u8, value: []const u8) StorageError!void {
        self.mu.lock();
        defer self.mu.unlock();

        const txn = self.env.transaction(.{
            .mode = .ReadWrite,
        }) catch return StorageError.TxnFailed;
        errdefer txn.abort() catch {};

        const db = txn.database(null, .{}) catch return StorageError.OpenFailed;

        db.set(key, value, .Upsert) catch |err| {
            return switch (err) {
                lmdbx.Error.MDBX_MAP_FULL => StorageError.DatabaseFull,
                else => StorageError.PutFailed,
            };
        };

        txn.commit() catch return StorageError.TxnFailed;
    }

    pub fn delete(self: *Storage, key: []const u8) StorageError!void {
        self.mu.lock();
        defer self.mu.unlock();

        const txn = self.env.transaction(.{
            .mode = .ReadWrite,
        }) catch return StorageError.TxnFailed;
        errdefer txn.abort() catch {};

        const db = txn.database(null, .{}) catch return StorageError.OpenFailed;

        db.delete(key) catch |err| {
            return switch (err) {
                lmdbx.Error.MDBX_NOTFOUND => StorageError.NotFound,
                else => StorageError.DeleteFailed,
            };
        };

        txn.commit() catch return StorageError.TxnFailed;
    }

    pub fn cursor(self: *Storage) StorageError!StorageCursor {
        self.mu.lock();
        defer self.mu.unlock();

        const txn = self.env.transaction(.{
            .mode = .ReadOnly,
        }) catch return StorageError.TxnFailed;

        const db = txn.database(null, .{}) catch {
            txn.abort() catch {};
            return StorageError.OpenFailed;
        };

        const cur = db.cursor() catch {
            txn.abort() catch {};
            return StorageError.GetFailed;
        };

        return .{
            .cursor = cur,
            .txn = txn,
            .allocator = self.allocator,
        };
    }

    pub fn sync(self: *Storage) StorageError!void {
        self.mu.lock();
        defer self.mu.unlock();

        self.env.sync() catch return StorageError.TxnFailed;
    }

    pub fn stat(self: *Storage) StorageError!lmdbx.Environment.Stat {
        self.mu.lock();
        defer self.mu.unlock();

        return self.env.stat() catch StorageError.GetFailed;
    }

    pub fn info(self: *Storage) StorageError!lmdbx.Environment.Info {
        self.mu.lock();
        defer self.mu.unlock();

        return self.env.info() catch StorageError.GetFailed;
    }
};

pub const StorageCursor = struct {
    cursor: lmdbx.Cursor,
    txn: lmdbx.Transaction,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *StorageCursor) void {
        self.cursor.deinit();
        self.txn.abort() catch {};
    }

    pub fn first(self: *StorageCursor) StorageError!?Entry {
        const key = self.cursor.goToFirst() catch |err| {
            return switch (err) {
                lmdbx.Error.MDBX_NOTFOUND => null,
                else => StorageError.GetFailed,
            };
        } orelse return null;

        _ = key;
        return self.cursor.getCurrentEntry() catch StorageError.GetFailed;
    }

    pub fn next(self: *StorageCursor) StorageError!?Entry {
        const key = self.cursor.goToNext() catch |err| {
            return switch (err) {
                lmdbx.Error.MDBX_NOTFOUND => null,
                else => StorageError.GetFailed,
            };
        } orelse return null;

        _ = key;
        return self.cursor.getCurrentEntry() catch StorageError.GetFailed;
    }

    pub fn seek(self: *StorageCursor, key: []const u8) StorageError!?Entry {
        const found_key = self.cursor.seek(key) catch |err| {
            return switch (err) {
                lmdbx.Error.MDBX_NOTFOUND => null,
                else => StorageError.GetFailed,
            };
        } orelse return null;

        _ = found_key;
        return self.cursor.getCurrentEntry() catch StorageError.GetFailed;
    }
};
