const std = @import("std");
const usearch = @import("usearch.zig");

const c = @cImport({
    @cInclude("usearch.h");
});

const Key = usearch.Key;
const Index = usearch.Index;
const IndexConfig = usearch.IndexConfig;
const distance = usearch.distance;
