const std = @import("std");
const rocksdb = @import("rocksdb");

const index = @import("index.zig");
const search = @import("search.zig");
const internals = @import("internals.zig");
const storageLib = @import("storage.zig");
const vectordb = @import("vectordb.zig");

const DB = rocksdb.DB;
const Data = rocksdb.Data;
const WriteBatch = rocksdb.WriteBatch;
const Storage = storageLib.Storage;

const AntarysDB = vectordb.AntarysDB;
const CollectionConfig = vectordb.CollectionConfig;
const DBConfig = vectordb.DBConfig;

const TESTING = true;

pub fn main() !void {
    if (TESTING) {
        const testKit = @import("./tests/testkit.zig");
        try testKit.testKit();

        return;
    }
}
