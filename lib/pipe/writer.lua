local httpclient = require("acid.httpclient")
local tableutil = require("acid.tableutil")
local strutil = require("acid.strutil")

local _M = { _VERSION = '1.0' }

local to_str = strutil.to_str

local BLOCK_SIZE = 1024 * 1024
local SOCKET_TIMEOUTS = {5 * 1000, 100 * 1000, 100 * 1000}

function _M.connect_http(ips, port, verb, uri, opts)
    opts = opts or {}

    local try_times = math.max(opts.try_times or 1, 1)

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

        http = httpclient:new(ip, port, opts.timeouts or SOCKET_TIMEOUTS)

        local h_opts = {method=req.verb, headers=req.headers}
        for i=1, try_times, 1 do
            err_code, err_msg = http:send_request(req.uri, h_opts)
            if err_code == nil then
                return http
            end
        end
    end

    if err_code ~= nil then
        err_msg  = to_str(err_code, ':', err_msg)
        err_code = 'ConnectError'
    end

    return nil, err_code, err_msg
end

function _M.loop_http_write(pobj, ident, http)
    local bytes = 0

    while true do
        local data, err_code, err_msg = pobj:read_pipe(ident)
        if err_code ~= nil then
            return nil, err_code, err_msg
        end

        if data == '' then
            break
        end

        local _, err_code, err_msg = http:send_body(data)
        if err_code ~= nil then
            return nil, err_code, err_msg
        end

        bytes = bytes + #data
    end

    return bytes
end

function _M.get_http_response(http)
    local err_code, err_msg = http:finish_request()
    if err_code ~= nil then
        return nil, err_code, err_msg
    end

    local resp = {
        status  = http.status,
        headers = http.headers,
    }
    local body = {}

    while true do
        local data, err_code, err_msg = http:read_body(BLOCK_SIZE)
        if err_code ~= nil then
            return resp, err_code, err_msg
        end

        if data == '' then
            break
        end

        table.insert(body, data)
    end
    resp.body = table.concat(body)

    return resp
end


function _M.make_file_writer(file_path, mode)
    return function(pobj, ident)
        local fp, err_msg = io.open(file_path, mode or 'w')
        if fp == nil then
            return nil, 'FileError', err_msg
        end

        local bytes = 0

        while true do
            local data, err_code, err_msg = pobj:read_pipe(ident)
            if err_code ~= nil then
                fp:close()
                return nil, err_code, err_msg
            end

            if data == '' then
                break
            end
            bytes = bytes + #data

            local rst, err_msg = fp:write(data)
            if rst == nil then
                fp:close()
                return nil, 'FileError', err_msg
            end
        end

        fp:close()

        return bytes
    end
end

function _M.make_connected_http_writer(http)
    return function(pobj, ident)
        local _, err_code, err_msg = _M.loop_http_write(pobj, ident, http)
        if err_code ~= nil then
            return nil, err_code, err_msg
        end

        return _M.get_http_response(http)
    end
end

function _M.make_http_writer(ips, port, verb, uri, opts)
    return function(pobj, ident)
        local http , err_code, err_msg = _M.connect_http(ips, port, verb, uri, opts)
        if err_code ~= nil then
            return nil, err_code, err_msg
        end

        local _, err_code, err_msg = _M.loop_http_write(pobj, ident, http)
        if err_code ~= nil then
            return nil, err_code, err_msg
        end

        return _M.get_http_response(http)
    end
end

function _M.make_ngx_writer()
    return function(pobj, ident)
        local bytes = 0

        while true do
            local data, err_code, err_msg = pobj:read_pipe(ident)
            if err_code ~= nil then
                return nil, err_code, err_msg
            end

            if data == '' then
                break
            end

            bytes = bytes + #data

            ngx.print(data)
            local _, err = ngx.flush(true)
            if err then
                return nil, 'ClientAborted', err
            end
        end

        return bytes
    end
end


function _M.make_buffer_writer(buffer, do_concat)
    do_concat = (do_concat ~= false)
    if buffer == nil then
        return nil, 'InvalidArguments', 'buffer is nil'
    end

    return function(pobj, ident)
        local bytes = 0
        local bufs = {}

        while true do
            local data, err_code, err_msg = pobj:read_pipe(ident)
            if err_code ~= nil then
                return nil, err_code, err_msg
            end

            if data == '' then
                break
            end

            bytes = bytes + #data
            table.insert(bufs, data)
        end

        if do_concat then
            buffer['buf'] = table.concat(bufs)
        else
            buffer['buf'] = bufs
        end

        return bytes
    end
end

return _M
