const std = @import("std");
const builtin = @import("builtin");

pub const ThreadPool = struct {
    allocator: std.mem.Allocator,
    threads: []std.Thread,
    queues: []WorkQueue,
    global_queue: *WorkQueue, // Heap allocated to avoid pointer invalidation
    shutdown: *std.atomic.Value(bool), // Heap allocated
    active_workers: *std.atomic.Value(usize), // Heap allocated
    affinity_enabled: bool,
    worker_contexts: []WorkerContext,

    const Self = @This();

    const Work = struct {
        func: *const fn (*anyopaque) void,
        context: *anyopaque,
    };

    const WorkerContext = struct {
        id: usize,
        queues: [*]WorkQueue,
        num_queues: usize,
        global_queue: *WorkQueue,
        shutdown: *std.atomic.Value(bool),
        active_workers: *std.atomic.Value(usize),
        allocator: std.mem.Allocator,
    };

    const WorkQueue = struct {
        buffer: []?Work,
        head: std.atomic.Value(usize),
        tail: std.atomic.Value(usize),
        mutex: std.Thread.Mutex,
        cond: std.Thread.Condition,
        allocator: std.mem.Allocator,

        fn init(allocator: std.mem.Allocator, capacity: usize) !WorkQueue {
            const actual_capacity = @max(capacity, 1) + 1; // +1 for empty/full distinction
            const buffer = try allocator.alloc(?Work, actual_capacity);
            @memset(buffer, null);

            return .{
                .buffer = buffer,
                .head = std.atomic.Value(usize).init(0),
                .tail = std.atomic.Value(usize).init(0),
                .mutex = .{},
                .cond = .{},
                .allocator = allocator,
            };
        }

        fn deinit(self: *WorkQueue) void {
            self.allocator.free(self.buffer);
        }

        fn push(self: *WorkQueue, work: Work) !void {
            self.mutex.lock();
            defer self.mutex.unlock();

            const head = self.head.load(.monotonic);
            const tail = self.tail.load(.monotonic);
            const capacity = self.buffer.len;

            if ((tail + 1) % capacity == head) {
                return error.QueueFull;
            }

            self.buffer[tail] = work;
            self.tail.store((tail + 1) % capacity, .release);
            self.cond.signal();
        }

        fn pop(self: *WorkQueue) ?Work {
            self.mutex.lock();
            defer self.mutex.unlock();

            const head = self.head.load(.monotonic);
            const tail = self.tail.load(.monotonic);

            if (head == tail) return null;

            const capacity = self.buffer.len;
            const work = self.buffer[head] orelse return null;

            self.buffer[head] = null;
            self.head.store((head + 1) % capacity, .release);
            return work;
        }

        fn steal(self: *WorkQueue) ?Work {
            self.mutex.lock();
            defer self.mutex.unlock();

            const head = self.head.load(.monotonic);
            var tail = self.tail.load(.monotonic);

            if (head == tail) return null;

            const capacity = self.buffer.len;
            tail = if (tail == 0) capacity - 1 else tail - 1;

            const work = self.buffer[tail] orelse return null;
            self.buffer[tail] = null;
            self.tail.store(tail, .release);
            return work;
        }

        fn waitForWork(self: *WorkQueue, shutdown: *std.atomic.Value(bool)) ?Work {
            self.mutex.lock();
            defer self.mutex.unlock();

            while (true) {
                const head = self.head.load(.monotonic);
                const tail = self.tail.load(.monotonic);

                if (head != tail) {
                    const capacity = self.buffer.len;
                    const work = self.buffer[head] orelse continue;

                    self.buffer[head] = null;
                    self.head.store((head + 1) % capacity, .release);
                    return work;
                }

                if (shutdown.load(.acquire)) {
                    return null;
                }

                self.cond.wait(&self.mutex);
            }
        }

        fn len(self: *WorkQueue) usize {
            self.mutex.lock();
            defer self.mutex.unlock();

            const head = self.head.load(.monotonic);
            const tail = self.tail.load(.monotonic);
            const capacity = self.buffer.len;

            return if (tail >= head)
                tail - head
            else
                capacity - head + tail;
        }

        fn isEmpty(self: *WorkQueue) bool {
            self.mutex.lock();
            defer self.mutex.unlock();
            return self.head.load(.monotonic) == self.tail.load(.monotonic);
        }
    };

    pub const Config = struct {
        /// Number of worker threads (0 = auto-detect CPU count)
        num_workers: usize = 0,
        /// Enable thread affinity (pin threads to CPUs)
        enable_affinity: bool = true,
        /// Per-thread queue capacity
        local_queue_capacity: usize = 32,
        /// Global queue capacity multiplier (workers * multiplier)
        global_queue_multiplier: usize = 16,
    };

    pub fn init(allocator: std.mem.Allocator, config: Config) !Self {
        const actual_workers = if (config.num_workers == 0)
            std.Thread.getCpuCount() catch 4
        else
            config.num_workers;

        const global_queue_capacity = actual_workers * config.global_queue_multiplier;

        // Allocate atomics on heap to avoid pointer invalidation when struct is returned
        const shutdown = try allocator.create(std.atomic.Value(bool));
        errdefer allocator.destroy(shutdown);
        shutdown.* = std.atomic.Value(bool).init(false);

        const active_workers = try allocator.create(std.atomic.Value(usize));
        errdefer allocator.destroy(active_workers);
        active_workers.* = std.atomic.Value(usize).init(0);

        // Allocate global queue on heap
        const global_queue = try allocator.create(WorkQueue);
        errdefer allocator.destroy(global_queue);
        global_queue.* = try WorkQueue.init(allocator, global_queue_capacity);
        errdefer global_queue.deinit();

        var pool = Self{
            .allocator = allocator,
            .threads = undefined,
            .queues = undefined,
            .global_queue = global_queue,
            .shutdown = shutdown,
            .active_workers = active_workers,
            .affinity_enabled = config.enable_affinity,
            .worker_contexts = undefined,
        };

        pool.threads = try allocator.alloc(std.Thread, actual_workers);
        errdefer allocator.free(pool.threads);

        pool.queues = try allocator.alloc(WorkQueue, actual_workers);
        errdefer allocator.free(pool.queues);

        pool.worker_contexts = try allocator.alloc(WorkerContext, actual_workers);
        errdefer allocator.free(pool.worker_contexts);

        // Initialize per-thread queues
        var queues_initialized: usize = 0;
        errdefer {
            for (pool.queues[0..queues_initialized]) |*queue| {
                queue.deinit();
            }
        }

        for (pool.queues) |*queue| {
            queue.* = try WorkQueue.init(allocator, config.local_queue_capacity);
            queues_initialized += 1;
        }

        // Initialize worker contexts (stable pointers for threads)
        for (pool.worker_contexts, 0..) |*ctx, i| {
            ctx.* = .{
                .id = i,
                .queues = pool.queues.ptr,
                .num_queues = pool.queues.len,
                .global_queue = pool.global_queue,
                .shutdown = pool.shutdown,
                .active_workers = pool.active_workers,
                .allocator = allocator,
            };
        }

        // Spawn worker threads
        var threads_spawned: usize = 0;
        errdefer {
            pool.shutdown.store(true, .release);

            pool.global_queue.mutex.lock();
            pool.global_queue.cond.broadcast();
            pool.global_queue.mutex.unlock();

            for (pool.queues[0..threads_spawned]) |*queue| {
                queue.mutex.lock();
                queue.cond.broadcast();
                queue.mutex.unlock();
            }

            for (pool.threads[0..threads_spawned]) |thread| {
                thread.join();
            }
        }

        for (0..actual_workers) |i| {
            pool.threads[i] = try std.Thread.spawn(.{}, worker, .{&pool.worker_contexts[i]});
            threads_spawned += 1;

            if (config.enable_affinity) {
                setThreadAffinity(pool.threads[i], i, actual_workers) catch |err| {
                    _ = err;
                };
            }
        }

        return pool;
    }

    pub fn deinit(self: *Self) void {
        self.shutdown.store(true, .release);

        // Wake all queues
        self.global_queue.mutex.lock();
        self.global_queue.cond.broadcast();
        self.global_queue.mutex.unlock();

        for (self.queues) |*queue| {
            queue.mutex.lock();
            queue.cond.broadcast();
            queue.mutex.unlock();
        }

        for (self.threads) |thread| {
            thread.join();
        }

        self.allocator.free(self.threads);
        self.allocator.free(self.worker_contexts);
        for (self.queues) |*queue| {
            queue.deinit();
        }
        self.allocator.free(self.queues);
        self.global_queue.deinit();
        self.allocator.destroy(self.global_queue);
        self.allocator.destroy(self.shutdown);
        self.allocator.destroy(self.active_workers);
    }

    pub fn submit(self: *Self, comptime func: fn (*anyopaque) void, context: *anyopaque) !void {
        if (self.shutdown.load(.acquire)) {
            return error.ShuttingDown;
        }

        const work = Work{ .func = func, .context = context };

        // Try to submit to global queue
        self.global_queue.push(work) catch {
            // If global queue is full, try local queues
            for (self.queues) |*queue| {
                queue.push(work) catch continue;
                return;
            }
            return error.QueueFull;
        };
    }

    pub fn waitIdle(self: *Self) void {
        while (true) {
            const active = self.active_workers.load(.acquire);
            if (active > 0) {
                std.Thread.yield() catch {};
                continue;
            }

            if (!self.global_queue.isEmpty()) {
                std.Thread.yield() catch {};
                continue;
            }

            var all_empty = true;
            for (self.queues) |*queue| {
                if (!queue.isEmpty()) {
                    all_empty = false;
                    break;
                }
            }

            if (all_empty) break;
            std.Thread.yield() catch {};
        }
    }

    fn worker(ctx: *WorkerContext) void {
        var rng = std.Random.DefaultPrng.init(@intCast(ctx.id));
        const random = rng.random();

        const queues = ctx.queues[0..ctx.num_queues];
        const my_queue = &queues[ctx.id];

        while (true) {
            // Try to get work from own queue first
            const work = my_queue.pop() orelse
                // Try global queue
                ctx.global_queue.pop() orelse
                // Try work stealing from other threads
                stealWork(ctx, random) orelse
                // Wait for work
                ctx.global_queue.waitForWork(ctx.shutdown) orelse break;

            // Execute work
            _ = ctx.active_workers.fetchAdd(1, .acq_rel);
            work.func(work.context);
            _ = ctx.active_workers.fetchSub(1, .acq_rel);
        }
    }

    fn stealWork(ctx: *WorkerContext, random: std.Random) ?Work {
        const queues = ctx.queues[0..ctx.num_queues];
        const num_workers = queues.len;
        const start = random.intRangeAtMost(usize, 0, num_workers - 1);

        var i: usize = 0;
        while (i < num_workers) : (i += 1) {
            const victim_id = (start + i) % num_workers;
            if (victim_id == ctx.id) continue;

            if (queues[victim_id].steal()) |work| {
                return work;
            }
        }

        return null;
    }

    /// Set thread affinity to bind thread to specific CPU cores
    fn setThreadAffinity(thread: std.Thread, worker_id: usize, total_workers: usize) !void {
        // Only supported on Linux and Windows
        switch (builtin.os.tag) {
            .linux => try setLinuxAffinity(thread, worker_id, total_workers),
            else => {}, // Unsupported platform - silently ignore
        }
    }

    fn setLinuxAffinity(thread: std.Thread, worker_id: usize, total_workers: usize) !void {
        if (builtin.os.tag != .linux) return;

        const cpu_count = std.Thread.getCpuCount() catch return;

        // Distribute workers across available CPUs
        // For NUMA awareness, ideally we'd query NUMA topology and assign
        // workers to nodes, but for now we do simple round-robin
        const cpu_id = (worker_id * cpu_count) / total_workers;

        // Use pthread_setaffinity_np via libc
        const c = @cImport({
            @cDefine("_GNU_SOURCE", "1");
            @cInclude("pthread.h");
            @cInclude("sched.h");
        });

        var cpu_set: c.cpu_set_t = undefined;
        c.CPU_ZERO(&cpu_set);
        c.CPU_SET(@intCast(cpu_id), &cpu_set);

        const pthread_handle = thread.getHandle();
        const result = c.pthread_setaffinity_np(pthread_handle, @sizeOf(c.cpu_set_t), &cpu_set);

        if (result != 0) {
            return error.AffinitySetFailed;
        }
    }

    pub fn runningWorkers(self: *Self) usize {
        return self.threads.len;
    }

    pub fn activeWorkers(self: *Self) usize {
        return self.active_workers.load(.acquire);
    }

    pub fn pendingTasks(self: *Self) usize {
        var total: usize = self.global_queue.len();
        for (self.queues) |*queue| {
            total += queue.len();
        }
        return total;
    }
};

pub const BatchCoordinator = struct {
    allocator: std.mem.Allocator,
    pool: *ThreadPool,
    errors: std.ArrayList(anyerror), // Unmanaged in Zig 0.15.1
    mutex: std.Thread.Mutex,
    completed: std.atomic.Value(usize),
    total: usize,
    completion_cond: std.Thread.Condition,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, pool: *ThreadPool, total: usize) !Self {
        return Self{
            .allocator = allocator,
            .pool = pool,
            .errors = std.ArrayList(anyerror).empty, // Use .empty for unmanaged
            .mutex = .{},
            .completed = std.atomic.Value(usize).init(0),
            .total = total,
            .completion_cond = .{},
        };
    }

    pub fn deinit(self: *Self) void {
        self.errors.deinit(self.allocator); // Pass allocator in 0.15.1
    }

    pub fn submitWork(self: *Self, comptime func: fn (*anyopaque) anyerror!void, context: *anyopaque) !void {
        const Wrapper = struct {
            coord: *BatchCoordinator,
            inner_func: *const fn (*anyopaque) anyerror!void,
            inner_context: *anyopaque,

            fn run(ptr: *anyopaque) void {
                const wrapper: *@This() = @ptrCast(@alignCast(ptr));
                defer wrapper.coord.allocator.destroy(wrapper);

                if (wrapper.inner_func(wrapper.inner_context)) {
                    // Success
                } else |err| {
                    wrapper.coord.mutex.lock();
                    wrapper.coord.errors.append(wrapper.coord.allocator, err) catch {}; // Pass allocator
                    wrapper.coord.mutex.unlock();
                }

                const new_completed = wrapper.coord.completed.fetchAdd(1, .acq_rel) + 1;

                if (new_completed == wrapper.coord.total) {
                    wrapper.coord.mutex.lock();
                    wrapper.coord.completion_cond.broadcast();
                    wrapper.coord.mutex.unlock();
                }
            }
        };

        const wrapper = try self.allocator.create(Wrapper);
        errdefer self.allocator.destroy(wrapper);

        wrapper.* = .{
            .coord = self,
            .inner_func = func,
            .inner_context = context,
        };

        try self.pool.submit(Wrapper.run, wrapper);
    }

    pub fn wait(self: *Self) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        while (self.completed.load(.acquire) < self.total) {
            self.completion_cond.wait(&self.mutex);
        }

        if (self.errors.items.len > 0) {
            return self.errors.items[0];
        }
    }

    pub fn completedCount(self: *Self) usize {
        return self.completed.load(.acquire);
    }

    pub fn errorCount(self: *Self) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.errors.items.len;
    }

    pub fn getErrors(self: *Self) []const anyerror {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.errors.items;
    }
};

/// NUMA-aware memory allocator wrapper (Linux only)
pub const NumaAllocator = struct {
    backing_allocator: std.mem.Allocator,
    node: ?usize,

    pub fn init(backing_allocator: std.mem.Allocator, node: ?usize) NumaAllocator {
        return .{
            .backing_allocator = backing_allocator,
            .node = node,
        };
    }

    pub fn allocator(self: *NumaAllocator) std.mem.Allocator {
        return .{
            .ptr = self,
            .vtable = &.{
                .alloc = alloc,
                .resize = resize,
                .free = free,
            },
        };
    }

    fn alloc(ctx: *anyopaque, len: usize, ptr_align: u8, ret_addr: usize) ?[*]u8 {
        const self: *NumaAllocator = @ptrCast(@alignCast(ctx));

        // TODO - If NUMA node specified and on Linux, try numa_alloc_onnode
        if (builtin.os.tag == .linux and self.node != null) {
            // NOTE - For production, use libnuma bindings here
        }

        return self.backing_allocator.rawAlloc(len, ptr_align, ret_addr);
    }

    fn resize(ctx: *anyopaque, buf: []u8, buf_align: u8, new_len: usize, ret_addr: usize) bool {
        const self: *NumaAllocator = @ptrCast(@alignCast(ctx));
        return self.backing_allocator.rawResize(buf, buf_align, new_len, ret_addr);
    }

    fn free(ctx: *anyopaque, buf: []u8, buf_align: u8, ret_addr: usize) void {
        const self: *NumaAllocator = @ptrCast(@alignCast(ctx));
        self.backing_allocator.rawFree(buf, buf_align, ret_addr);
    }
};
