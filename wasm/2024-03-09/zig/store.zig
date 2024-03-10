const std = @import("std");

// extern fn registerActor(actor: ?*anyopaque) void;
// extern fn processMessages(messages: [*]const u8, len: usize) [*]const u8;

var actors: std.ArrayList(?*anyopaque) = undefined;

pub export fn init() void {
    actors = std.ArrayList(?*anyopaque).init(std.heap.page_allocator);
}

pub export fn deinit() void {
    actors.deinit();
}

pub export fn registerActor(actor: ?*anyopaque) void {
    const result = try actors.append(actor);
    if (result != null) {
        // Handle the error here, e.g. log or return an error value
        return;
    }
}

pub export fn processMessages(messages_ptr: [*]const u8, messages_len: usize) [*]const u8 {
    var messages_slice = [][]const u8{messages_ptr[0..messages_len]};
    var result = std.ArrayList(u8).init(std.heap.page_allocator);
    // No error handling for simplicity in this interface
    for (messages_slice) |message| {
        try result.appendSlice(message);
    }
    return result.toOwnedSlice().ptr;
}
