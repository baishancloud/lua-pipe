local err_socket = require("err_socket")
local tableutil = require("acid.tableutil")
local strutil = require("acid.strutil")
local httpclient = require("acid.httpclient")

local to_str = strutil.to_str

local _M = { _VERSION = '1.0' }

local BLOCK_SIZE = 1024 * 1024
local SOCKET_TIMEOUT = 100 * 1000

function _M.make_http_reader(ips, port, verb, uri, opts)
    opts = opts or {}

    local ret = {
        size = 0,
        time = 0,
    }

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

            http = httpclient:new(ip, port, opts.timeout or SOCKET_TIMEOUT)

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
            local t0 = ngx.now()
            local buf, err_code, err_msg =
                http:read_body(opts.block_size or BLOCK_SIZE)
            ret.time = ret.time + (ngx.now() - t0)
            if err_code ~= nil then
                return nil, err_code, err_msg
            end

            local rst, err_code, err_msg = pobj:write_pipe(ident, buf)
            if err_code ~= nil then
                return nil, err_code, err_msg
            end

            ret.size = ret.size + #buf

            if buf == '' then
                break
            end
        end
    end
end

function _M.make_socket_reader(socket, size, block_size)
    block_size = block_size or BLOCK_SIZE

    local ret = {
        size = 0,
        time = 0,
    }

    return function(pobj, ident)
        local buf, rst, err_code, err_msg

        while true do
            local recv_size = math.min(size, block_size)

            if recv_size == 0 then
                buf = ''
            else
                local t0 = ngx.now()
                buf, err_msg = socket:receive(recv_size)
                ret.time = ret.time + (ngx.now() - t0)
                if buf == nil then
                    return nil, err_socket.to_code(err_msg), 'socket error: ' .. err_msg
                end
            end

            rst, err_code, err_msg = pobj:write_pipe(ident, buf)
            if err_code ~= nil then
                return nil, err_code, err_msg
            end

            size = size - #buf
            ret.size = ret.size + #buf

            if buf == '' then
                break
            end
        end

        return ret
    end
end


function _M.make_file_reader(fpath, block_size)
    block_size = block_size or BLOCK_SIZE

    local ret = {
        size = 0,
        time = 0,
    }

    return function(pobj, ident)
        local buf

        local fp, err_msg = io.open(fpath, 'r')
        if fp == nil then
            return nil, 'FileError', err_msg
        end

        while true do
            local t0 = ngx.now()
            buf = fp:read(block_size)
            ret.time = ret.time + (ngx.now() - t0)

            buf = buf or ''

            local _, err_code, err_msg = pobj:write_pipe(ident, buf)
            if err_code ~= nil then
                fp:close()
                return nil, err_code, err_msg
            end

            ret.size = ret.size + #buf

            if buf == '' then
                break
            end
        end

        fp:close()

        return ret
    end
end


return _M
