local pipe_pipe   = require("cpipe.pipe")
local pipe_reader = require("cpipe.reader")
local pipe_writer = require("cpipe.writer")

local headers  = ngx.req.get_headers()
local size = tonumber(headers['Content-Length'])

local socket_reader = pipe_reader.make_socket_reader(ngx.socket.tcp(), size)

local writers = {}
for _, fpath in ipairs({
    '/tmp/t1.out',
    '/tmp/t2.out',
    '/tmp/t3.out'}) do

    local file_writer = pipe_writer.make_file_writer(fpath)
    table.insert(writers, file_writer)
end

local cpipe = pipe_pipe:new({socket_reader}, writers)

local is_running = function() return true end

local rst, err_code, err_msg = cpipe:pipe(is_running)
if err_code ~= nil then
    return nil, err_code, err_msg
end
