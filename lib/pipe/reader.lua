local tableutil = require("acid.tableutil")
local httplib = require("pipe.httplib")

local _M = { _VERSION = '1.0' }

local BLOCK_SIZE = 1024 * 1024

local err_socket = {
    [ "default" ]        = "InvalidRequest",
    [ "timeout" ]        = "RequestTimeout",
    [ "client aborted" ] = "InvalidRequest",
    [ "connection reset by peer" ] = "InvalidRequest",
}

local function socket_err_code( err, default )
    default = default or _M.default
    return err_socket[err] or default
end

function _M.connect_http(ips, port, verb, uri, opts)
    return httplib.connect_http(ips, port, verb, uri, opts)
end

function _M.get_http_response(http, opts)
    return httplib.get_http_response(http, opts)
end

function _M.loop_http_read(pobj, ident, http)
    return httplib.loop_http_read(pobj, ident, http)
end

function _M.make_connected_http_reader(http)
    return function(pobj, ident)
        return _M.loop_http_read(pobj, ident, http)
    end
end

function _M.make_http_reader(ips, port, verb, uri, opts)
    return function(pobj, ident)
        local http , err_code, err_msg = _M.connect_http(ips, port, verb, uri, opts)
        if err_code ~= nil then
            return nil, err_code, err_msg
        end

        opts = tableutil.dup(opts, true)
        opts.read_body = false

        local _, err_code, err_msg = _M.get_http_response(http, opts)
        if err_code ~= nil then
            return nil, err_code, err_msg
        end

        return _M.loop_http_read(pobj, ident, http)
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
                    return nil, socket_err_code(err_msg), 'socket error: ' .. err_msg
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

function _M.make_memery_reader(bufs)
    local ret = {
        size = 0,
        time = 0,
    }

    if type(bufs) == type('') then
        bufs = {bufs}
    end

    table.insert(bufs, '')

    return function(pobj, ident)
        for _, buf in ipairs(bufs) do
            local _, err_code, err_msg = pobj:write_pipe(ident, buf)
            if err_code ~= nil then
                return nil, err_code, err_msg
            end

            ret.size = ret.size + #buf

            if buf == '' then
                break
            end
        end

        return ret
    end
end

return _M
