const std = @import("std");

const testVectorDB = @import("test_vectordb.zig");

pub fn testKit() !void {
    const allocator = std.heap.page_allocator;
    try testVectorDB.runVectorDBTests(allocator);
}
