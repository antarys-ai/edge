const std = @import("std");

const Allocator = std.mem.Allocator;

pub fn List(comptime T: type) type {
    return struct {
        head: ?*Node,
        tail: ?*Node,
        mutex: std.Thread.Mutex,

        pub const Node = struct {
            value: T,
            prev: ?*Node = null,
            next: ?*Node = null,
        };

        const Self = @This();

        pub fn init() Self {
            return .{
                .head = null,
                .tail = null,
                .mutex = .{},
            };
        }

        pub fn insert(self: *Self, node: *Node) void {
            self.mutex.lock();
            defer self.mutex.unlock();
            self.moveToFrontLocked(node);
        }

        pub fn moveToFront(self: *Self, node: *Node) void {
            self.mutex.lock();
            defer self.mutex.unlock();
            self.removeLocked(node);
            self.moveToFrontLocked(node);
        }

        pub fn moveToTail(self: *Self, node: *Node) void {
            self.mutex.lock();
            defer self.mutex.unlock();
            self.removeLocked(node);
            self.moveToTailLocked(node);
        }

        pub fn remove(self: *Self, node: *Node) void {
            self.mutex.lock();
            self.removeLocked(node);
            self.mutex.unlock();
            node.next = null;
            node.prev = null;
        }

        pub fn removeTail(self: *Self) ?*Node {
            if (self.tail) |node| {
                if (node.prev) |prev| {
                    self.tail = prev;
                    prev.next = null;
                } else {
                    self.tail = null;
                    self.head = null;
                }
                node.next = null;
                node.prev = null;
                return node;
            } else {
                return null;
            }
        }

        fn moveToFrontLocked(self: *Self, node: *Node) void {
            if (self.head) |head| {
                head.prev = node;
                node.next = head;
                self.head = node;
            } else {
                self.head = node;
                self.tail = node;
            }
            node.prev = null;
        }

        fn moveToTailLocked(self: *Self, node: *Node) void {
            if (self.tail) |tail| {
                tail.next = node;
                node.prev = tail;
                self.tail = node;
            } else {
                self.head = node;
                self.tail = node;
            }
            node.next = null;
        }

        fn removeLocked(self: *Self, node: *Node) void {
            if (node.prev) |prev| {
                prev.next = node.next;
            } else {
                self.head = node.next;
            }

            if (node.next) |next| {
                next.prev = node.prev;
            } else {
                self.tail = node.prev;
            }
        }
    };
}
