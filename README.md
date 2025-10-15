## Antarys Edge

Next generation Antarys vector db backed by Usearch HNSW and RocksDB for scale.

### Internal ThreadPool API

```zig
const std = @import("std");
const ThreadPool = @import("threadpool.zig").ThreadPool;
const BatchCoordinator = @import("threadpool.zig").BatchCoordinator;

const ComputeTask = struct {
    id: usize,
    data: []f64,
    result: *std.atomic.Value(f64),

    fn execute(ctx: *anyopaque) void {
        const self: *ComputeTask = @ptrCast(@alignCast(ctx));

        var sum: f64 = 0.0;
        for (self.data) |val| {
            sum += @sqrt(val * val + 1.0);
        }

        self.result.store(sum, .release);
        std.debug.print("Task {} completed: {d:.2}\n", .{ self.id, sum });
    }
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("\nExample 1: Basic ThreadPool \n", .{});
    try basicExample(allocator);

    std.debug.print("\nExample 2: Batch Coordinator \n", .{});
    try batchExample(allocator);

    std.debug.print("\nExample 3: Work Stealing \n", .{});
    try workStealingExample(allocator);

    std.debug.print("\nExample 4: NUMA Configuration \n", .{});
    try numaExample(allocator);
}

fn basicExample(allocator: std.mem.Allocator) !void {
    var pool = try ThreadPool.init(allocator, .{
        .num_workers = 4,
        .enable_affinity = true,
    });
    defer pool.deinit();

    std.debug.print("Pool initialized with {} workers\n", .{pool.runningWorkers()});

    const num_tasks = 8;
    var results: [num_tasks]std.atomic.Value(f64) = undefined;
    var tasks: [num_tasks]ComputeTask = undefined;

    var all_data: [num_tasks][]f64 = undefined;
    defer {
        for (all_data) |data| {
            allocator.free(data);
        }
    }

    for (&tasks, 0..) |*task, i| {
        results[i] = std.atomic.Value(f64).init(0.0);

        const data = try allocator.alloc(f64, 1000);
        all_data[i] = data;
        for (data, 0..) |*d, j| {
            d.* = @as(f64, @floatFromInt(i * 1000 + j));
        }

        task.* = .{
            .id = i,
            .data = data,
            .result = &results[i],
        };

        try pool.submit(ComputeTask.execute, task);
    }

    pool.waitIdle();

    std.debug.print("All tasks completed!\n", .{});
    std.debug.print("Pending tasks: {}\n", .{pool.pendingTasks()});
    std.debug.print("Active workers: {}\n", .{pool.activeWorkers()});
}

fn batchExample(allocator: std.mem.Allocator) !void {
    var pool = try ThreadPool.init(allocator, .{});
    defer pool.deinit();

    const BatchTask = struct {
        id: usize,
        should_fail: bool,

        fn execute(ctx: *anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));

            if (self.should_fail) {
                return error.TaskFailed;
            }

            std.Thread.sleep(100 * std.time.ns_per_ms);
            std.debug.print("Batch task {} succeeded\n", .{self.id});
        }
    };

    const num_tasks = 10;
    var coordinator = try BatchCoordinator.init(allocator, &pool, num_tasks);
    defer coordinator.deinit();

    var tasks: [num_tasks]BatchTask = undefined;
    for (&tasks, 0..) |*task, i| {
        task.* = .{
            .id = i,
            .should_fail = (i == 5),
        };
        try coordinator.submitWork(BatchTask.execute, task);
    }

    coordinator.wait() catch |err| {
        std.debug.print("Batch failed with error: {}\n", .{err});
        std.debug.print("Completed: {}/{}\n", .{ coordinator.completedCount(), num_tasks });
        std.debug.print("Errors: {}\n", .{coordinator.errorCount()});
        return;
    };

    std.debug.print("Batch completed successfully!\n", .{});
}

fn workStealingExample(allocator: std.mem.Allocator) !void {
    var pool = try ThreadPool.init(allocator, .{
        .num_workers = 2,
        .local_queue_capacity = 4,
    });
    defer pool.deinit();

    const QuickTask = struct {
        id: usize,
        worker_hint: usize,

        fn execute(ctx: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            std.debug.print("Task {} executing\n", .{self.id});

            var sum: u64 = 0;
            var i: u64 = 0;
            while (i < 1000) : (i += 1) {
                sum += i;
            }
        }
    };

    const num_tasks = 20;
    var tasks: [num_tasks]QuickTask = undefined;

    for (&tasks, 0..) |*task, i| {
        task.* = .{
            .id = i,
            .worker_hint = i % 2,
        };
        try pool.submit(QuickTask.execute, task);
    }

    pool.waitIdle();
    std.debug.print("Work stealing example completed!\n", .{});
}

fn numaExample(allocator: std.mem.Allocator) !void {
    const cpu_count = std.Thread.getCpuCount() catch 4;
    std.debug.print("Detected {} CPUs\n", .{cpu_count});

    var pool = try ThreadPool.init(allocator, .{
        .num_workers = cpu_count,
        .enable_affinity = false,
        .local_queue_capacity = 64,
        .global_queue_multiplier = 16,
    });
    defer pool.deinit();

    std.debug.print("NUMA-aware pool initialized\n", .{});
    std.debug.print("Thread affinity: enabled\n", .{});
    std.debug.print("Workers: {}\n", .{pool.runningWorkers()});

    const NumaTask = struct {
        id: usize,

        fn execute(ctx: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));

            std.debug.print("NUMA task {} running\n", .{self.id});

            var i: usize = 0;
            while (i < 10000) : (i += 1) {}
        }
    };

    var tasks: [16]NumaTask = undefined;
    for (&tasks, 0..) |*task, i| {
        task.* = .{ .id = i };
        try pool.submit(NumaTask.execute, task);
    }

    pool.waitIdle();
    std.debug.print("NUMA example completed!\n", .{});
}

fn numaAllocatorExample(allocator: std.mem.Allocator) !void {
    const NumaAllocator = @import("threadpool.zig").NumaAllocator;

    var numa_alloc = NumaAllocator.init(allocator, 0);
    const numa_allocator = numa_alloc.allocator();

    const data = try numa_allocator.alloc(u64, 1024);
    defer numa_allocator.free(data);
}

const PoolStats = struct {
    start_time: i64,
    tasks_submitted: usize,
    tasks_completed: usize,

    fn create() PoolStats {
        return .{
            .start_time = std.time.milliTimestamp(),
            .tasks_submitted = 0,
            .tasks_completed = 0,
        };
    }

    fn printStats(self: *const PoolStats, pool: *ThreadPool) void {
        const elapsed = std.time.milliTimestamp() - self.start_time;
        const throughput = if (elapsed > 0)
            @as(f64, @floatFromInt(self.tasks_completed * 1000)) / @as(f64, @floatFromInt(elapsed))
        else
            0.0;

        std.debug.print("\nPool Statistics \n", .{});
        std.debug.print("Elapsed: {}ms\n", .{elapsed});
        std.debug.print("Tasks submitted: {}\n", .{self.tasks_submitted});
        std.debug.print("Tasks completed: {}\n", .{self.tasks_completed});
        std.debug.print("Throughput: {d:.2} tasks/sec\n", .{throughput});
        std.debug.print("Pending: {}\n", .{pool.pendingTasks()});
        std.debug.print("Active workers: {}\n", .{pool.activeWorkers()});
    }
};
```
