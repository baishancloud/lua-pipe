local s2http = require("s2http")
local tableutil = require("acid.tableutil")

local _M = { _VERSION = '1.0' }

local BLOCK_SIZE = 1024 * 1024
local SOCKET_TIMEOUT = 100 * 1000

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


function _M.make_http_writer(ips, port, verb, uri, opts)
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
                err_code, err_msg = http:send_request(req.uri, h_opts)
                if err_code == nil then
                    break
                end
            end

            if err_code ~= nil then
                return nil, err_code, err_msg
            end
        end

        while true do
            local data, err_code, err_msg = pobj:read_pipe(ident)
            if err_code ~= nil then
                return nil, err_code, err_msg
            end

            if data == '' then
                break
            end

            local bytes, err_code, err_msg = http:send_body(data)
            if err_code ~= nil then
                return nil, err_code, err_msg
            end
        end

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

return _M
