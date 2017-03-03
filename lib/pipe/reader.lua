local err_socket = require("err_socket")
local tableutil = require("acid.tableutil")
local s2http = require("s2http")

local _M = { _VERSION = '1.0' }
local mt = { __index = _M }

local BLOCK_SIZE = 1024 * 1024
local SOCKET_TIMEOUT = 100 * 1000

function _M.make_http_reader(ips, port, verb, uri, opts)
    opts = opts or {}

    return function(pobj, ident)
        local http, err_code, err_msg

        for _, ip in ipairs(ips) do
            local headers = tableutil.dup(opts.headers or {}, true)
            headers.Host = headers.Host or ip

            local req = {
                ip   = ip,
                port = port,
                uri  = uri,
                verb = verb,
                headers = headers,
            }

            if opts.signature_cb ~= nil then
                req = opts.signature_cb(req)
            end

            http = s2http:new(ip, port, opts.timeout or SOCKET_TIMEOUT)

            local h_opts = {method=req.verb, headers=req.headers}
            for i=1, 3, 1 do
                err_code, err_msg = http:request(req.uri, h_opts)
                if err_code == nil then
                    break
                end
            end

            if err_code ~= nil then
                return nil, err_code, err_msg
            end
        end

        while true do
            local buf, err_code, err_msg =
                http:read_body(opts.block_size or BLOCK_SIZE)
            if err_code ~= nil then
                return nil, err_code, err_msg
            end

            local rst, err_code, err_msg = pobj:write_pipe(ident, buf)
            if err_code ~= nil then
                return nil, err_code, err_msg
            end

            if buf == '' then
                break
            end
        end
    end
end

function _M.make_socket_reader(socket, size, block_size)
    block_size = block_size or BLOCK_SIZE

    return function(pobj, ident)
        local buf, rst, err_code, err_msg

        while true do
            local recv_size = math.min(size, block_size)

            if recv_size == 0 then
                buf = ''
            else
                buf, err_msg = socket:receive(recv_size)
                if buf == nil then
                    return nil, err_socket.to_code(err_msg), 'socket error: ' .. err_msg
                end
            end

            rst, err_code, err_msg = pobj:write_pipe(ident, buf)
            if err_code ~= nil then
                return nil, err_code, err_msg
            end

            size = size - #buf

            if buf == '' then
                break
            end
        end
    end
end

return _M
