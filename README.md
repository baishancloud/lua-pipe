#   Name

lua-pipe

#   Status

This library is in alpha phase.

It is deployed in a test envinroment, after add completely test case,
production environment in the movement

#   Description

Lua implementation data stream N input and M output piping

#   Synopsis

```lua

local pipe_pipe = require("pipe.pipe")

local headers  = ngx.req.get_headers()
local size = tonumber(headers['Content-Length'])

local socket_reader = pipe_pipe.reader.make_socket_reader(ngx.socket.tcp(), size)

local writers = {}
for _, fpath in ipairs({
    '/tmp/t1.out',
    '/tmp/t2.out',
    '/tmp/t3.out'}) do

    local file_writer = pipe_pipe.writer.make_file_writer(fpath)
    table.insert(writers, file_writer)
end

local cpipe = pipe_pipe:new({socket_reader}, writers)

local is_running = function() return true end

local rst, err_code, err_msg = cpipe:pipe(is_running)
if err_code ~= nil then
    return nil, err_code, err_msg
end

```
# modules

#   Author

Wu Yipu (吴义谱) <pengsven@gmail.com>

#   Copyright and License

The MIT License (MIT)

Copyright (c) 2017 Wu Yipu (吴义谱) <pengsven@gmail.com>

