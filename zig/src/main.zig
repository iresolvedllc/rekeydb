const std = @import("std");
const Io = std.Io;

const help =
    \\rekeydb-cli - RESP client for rekeydb
    \\
    \\Usage: rekeydb-cli [OPTIONS] [COMMAND [ARG ...]]
    \\
    \\Options:
    \\  --host <host>   Server host (default: 127.0.0.1)
    \\  --port <port>   Server port (default: 6379)
    \\  --raw           Print bulk strings without quoting
    \\  --version       Print version and exit
    \\  --help          Print this help and exit
    \\
;

pub fn main(init: std.process.Init) !void {
    const arena = init.arena.allocator();
    const args = try init.minimal.args.toSlice(arena);
    const io = init.io;

    var host: []const u8 = "127.0.0.1";
    var port: u16 = 6379;
    var raw = false;
    var command_start: ?usize = null;

    var index: usize = 1;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            try stdoutPrint(io, help);
            return;
        } else if (std.mem.eql(u8, arg, "--version") or std.mem.eql(u8, arg, "-v")) {
            try stdoutPrint(io, "rekeydb-cli 0.1.0\n");
            return;
        } else if (std.mem.eql(u8, arg, "--host")) {
            index += 1;
            if (index >= args.len) return error.MissingHost;
            host = args[index];
        } else if (std.mem.eql(u8, arg, "--port")) {
            index += 1;
            if (index >= args.len) return error.MissingPort;
            port = try std.fmt.parseInt(u16, args[index], 10);
        } else if (std.mem.eql(u8, arg, "--raw")) {
            raw = true;
        } else if (std.mem.startsWith(u8, arg, "-")) {
            return error.UnknownOption;
        } else {
            command_start = index;
            break;
        }
    }

    const address = try std.Io.net.IpAddress.parse(host, port);
    var stream = try address.connect(io, .{ .mode = .stream });
    defer stream.close(io);

    var read_buffer: [8192]u8 = undefined;
    var write_buffer: [8192]u8 = undefined;
    var reader = stream.reader(io, &read_buffer);
    var writer = stream.writer(io, &write_buffer);

    const command: []const []const u8 = if (command_start) |start|
        args[start..]
    else
        &[_][]const u8{"PING"};

    try writeCommand(&writer.interface, command);
    try writer.interface.flush();
    try printReply(&reader.interface, io, raw);
}

fn stdoutPrint(io: Io, text: []const u8) !void {
    var buffer: [4096]u8 = undefined;
    var writer = Io.File.stdout().writer(io, &buffer);
    try writer.interface.writeAll(text);
    try writer.interface.flush();
}

fn writeCommand(writer: *Io.Writer, args: []const []const u8) !void {
    try writer.print("*{d}\r\n", .{args.len});
    for (args) |arg| {
        try writer.print("${d}\r\n", .{arg.len});
        try writer.writeAll(arg);
        try writer.writeAll("\r\n");
    }
}

fn readLine(reader: *Io.Reader) ![]u8 {
    const line = try reader.takeDelimiterInclusive('\n');
    if (line.len == 0 or line[line.len - 1] != '\n') return error.BadReply;
    var end = line.len - 1;
    if (end > 0 and line[end - 1] == '\r') end -= 1;
    return line[0..end];
}

fn printReply(reader: *Io.Reader, io: Io, raw: bool) !void {
    var buffer: [8192]u8 = undefined;
    var stdout = Io.File.stdout().writer(io, &buffer);
    const writer = &stdout.interface;
    defer writer.flush() catch {};

    var indent: usize = 0;
    try printValue(reader, writer, raw, &indent);
    try writer.flush();
}

fn printValue(reader: *Io.Reader, writer: *Io.Writer, raw: bool, indent: *usize) !void {
    const line = try readLine(reader);
    if (line.len == 0) return error.BadReply;
    const tag = line[0];
    const payload = line[1..];
    switch (tag) {
        '+' => try writer.print("{s}\n", .{payload}),
        '-' => try writer.print("(error) {s}\n", .{payload}),
        ':' => try writer.print("{s}\n", .{payload}),
        '$' => {
            const length = try std.fmt.parseInt(i64, payload, 10);
            if (length < 0) {
                try writer.writeAll("(nil)\n");
                return;
            }
            const len: usize = @intCast(length);
            const data = try reader.readAlloc(std.heap.page_allocator, len);
            defer std.heap.page_allocator.free(data);
            var crlf: [2]u8 = undefined;
            try reader.readSliceAll(&crlf);
            if (crlf[0] != '\r' or crlf[1] != '\n') return error.BadReply;
            if (raw) {
                try writer.writeAll(data);
                try writer.writeAll("\n");
            } else {
                try writer.print("\"{s}\"\n", .{data});
            }
        },
        '*' => {
            const count = try std.fmt.parseInt(i64, payload, 10);
            if (count < 0) {
                try writer.writeAll("(nil)\n");
                return;
            }
            if (count == 0) {
                try writer.writeAll("(empty array)\n");
                return;
            }
            var i: usize = 0;
            while (i < @as(usize, @intCast(count))) : (i += 1) {
                try writer.print("{d}) ", .{i + 1});
                indent.* += 1;
                try printValue(reader, writer, raw, indent);
                indent.* -= 1;
            }
        },
        '_' => try writer.writeAll("(nil)\n"),
        '#' => try writer.print("{s}\n", .{payload}),
        ',' => try writer.print("{s}\n", .{payload}),
        '(' => try writer.print("{s}\n", .{payload}),
        '=' => {
            const length = try std.fmt.parseInt(i64, payload, 10);
            if (length < 0) {
                try writer.writeAll("(nil)\n");
                return;
            }
            const len: usize = @intCast(length);
            const data = try reader.readAlloc(std.heap.page_allocator, len);
            defer std.heap.page_allocator.free(data);
            var crlf: [2]u8 = undefined;
            try reader.readSliceAll(&crlf);
            try writer.print("{s}\n", .{data});
        },
        '%', '~', '>' => {
            const count = try std.fmt.parseInt(i64, payload, 10);
            if (count < 0) {
                try writer.writeAll("(nil)\n");
                return;
            }
            var i: usize = 0;
            while (i < @as(usize, @intCast(count))) : (i += 1) {
                try printValue(reader, writer, raw, indent);
                try printValue(reader, writer, raw, indent);
            }
        },
        else => return error.BadReply,
    }
}

test "write command encodes a RESP array" {
    var buffer: [128]u8 = undefined;
    var writer = Io.Writer.fixed(&buffer);
    try writeCommand(&writer, &[_][]const u8{ "SET", "k", "v" });
    try std.testing.expectEqualStrings("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n", buffer[0..writer.end]);
}
