const std = @import("std");

pub const Entry = @import("entry.zig").Entry;
const Segment = @import("segment.zig").Segment;

const Allocator = std.mem.Allocator;

pub const Config = struct {
    max_size: u32 = 8000,
    segment_count: u16 = 8,
    gets_per_promote: u8 = 5,
    shrink_ratio: f32 = 0.2,
};

pub const PutConfig = struct {
    ttl: u32 = 300,
    size: u32 = 1,
};

pub fn Cache(comptime T: type) type {
    return struct {
        allocator: Allocator,
        segment_mask: u16,
        segments: []Segment(T),

        const Self = @This();

        pub fn init(allocator: Allocator, config: Config) !Self {
            const segment_count = config.segment_count;
            if (segment_count == 0) return error.SegmentBucketNotPower2;
            // has to be a power of 2
            if ((segment_count & (segment_count - 1)) != 0) return error.SegmentBucketNotPower2;

            const shrink_ratio = config.shrink_ratio;
            if (shrink_ratio == 0 or shrink_ratio > 1) return error.SpaceToFreeInvalid;

            const segment_max_size = config.max_size / segment_count;
            const segment_config = .{
                .max_size = segment_max_size,
                .target_size = segment_max_size - @as(u32, @intFromFloat(@as(f32, @floatFromInt(segment_max_size)) * shrink_ratio)),
                .gets_per_promote = config.gets_per_promote,
            };

            const segments = try allocator.alloc(Segment(T), segment_count);
            for (0..segment_count) |i| {
                segments[i] = Segment(T).init(allocator, segment_config);
            }

            return .{
                .allocator = allocator,
                .segments = segments,
                .segment_mask = segment_count - 1,
            };
        }

        pub fn deinit(self: *Self) void {
            const allocator = self.allocator;
            for (self.segments) |*segment| {
                segment.deinit();
            }
            allocator.free(self.segments);
        }

        pub fn contains(self: *const Self, key: []const u8) bool {
            return self.getSegment(key).contains(key);
        }

        pub fn get(self: *Self, key: []const u8) ?*Entry(T) {
            return self.getSegment(key).get(key);
        }

        pub fn getEntry(self: *const Self, key: []const u8) ?*Entry(T) {
            return self.getSegment(key).getEntry(key);
        }

        pub fn put(self: *Self, key: []const u8, value: T, config: PutConfig) !void {
            _ = try self.getSegment(key).put(self.allocator, key, value, config);
        }

        pub fn del(self: *Self, key: []const u8) bool {
            return self.getSegment(key).del(key);
        }

        pub fn delPrefix(self: *Self, prefix: []const u8) !usize {
            var total: usize = 0;
            const allocator = self.allocator;
            for (self.segments) |*segment| {
                total += try segment.delPrefix(allocator, prefix);
            }
            return total;
        }

        pub fn fetch(self: *Self, comptime S: type, key: []const u8, loader: *const fn (state: S, key: []const u8) anyerror!?T, state: S, config: PutConfig) !?*Entry(T) {
            return self.getSegment(key).fetch(S, self.allocator, key, loader, state, config);
        }

        pub fn maxSize(self: Self) usize {
            return self.segments[0].max_size * self.segments.len;
        }

        fn getSegment(self: *const Self, key: []const u8) *Segment(T) {
            const hash_code = std.hash.Wyhash.hash(0, key);
            return &self.segments[hash_code & self.segment_mask];
        }
    };
}
