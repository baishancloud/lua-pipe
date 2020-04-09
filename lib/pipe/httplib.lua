local httpclient = require("acid.httpclient")
local tableutil = require("acid.tableutil")
local strutil = require("acid.strutil")

local resty_sha1 = require( "resty.sha1" )
local resty_md5 = require( "resty.md5" )
local resty_string = require( "resty.string" )

local _M = { _VERSION = '1.0' }

local to_str = strutil.to_str

local BLOCK_SIZE = 1024 * 1024
local SOCKET_TIMEOUTS = {5 * 1000, 100 * 1000, 100 * 1000}

function _M.connect_http(ips, port, verb, uri, opts)
    opts = opts or {}

    local try_times = math.max(opts.try_times or 1, 1)

    local http, _, err_code, err_msg

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

        http = httpclient:new(ip, port, opts.timeouts or SOCKET_TIMEOUTS, opts.http_opts)

        local h_opts = {method=req.verb, headers=req.headers}
        for i=1, try_times, 1 do
            _, err_code, err_msg = http:send_request(req.uri, h_opts)
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

function _M.get_http_response(http, opts)
    opts = opts or {}

    local _, err_code, err_msg = http:finish_request()
    if err_code ~= nil then
        return nil, err_code, err_msg
    end

    if opts.success_status ~= nil and opts.success_status ~= http.status then
        return nil, 'InvalidHttpStatus', to_str('response http status:', http.status)
    end

    local resp = {
        status  = http.status,
        headers = http.ori_headers,
    }

    if opts.read_body == false then
        return resp
    end

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

function _M.loop_http_read(pobj, ident, http, block_size, opts)
    opts = opts or {}

    local rst = {
        size = 0,
        md5 = nil,
        sha1 = nil,
    }

    local md5_alg
    if opts.calc_md5 == true then
        md5_alg = resty_md5:new()
    end

    local sha1_alg
    if opts.calc_sha1 == true then
        sha1_alg = resty_sha1:new()
    end

    while true do
        local data, err_code, err_msg =
            http:read_body(block_size or BLOCK_SIZE)
        if err_code ~= nil then
            return nil, err_code, err_msg
        end

        local _, err_code, err_msg = pobj:write_pipe(ident, data)
        if err_code ~= nil then
            return nil, err_code, err_msg
        end

        if opts.calc_md5 == true then
            md5_alg:update(data)
        end

        if opts.calc_sha1 == true then
            sha1_alg:update(data)
        end

        rst.size = rst.size + #data

        if data == '' then
            if opts.calc_md5 == true then
                rst.md5 = resty_string.to_hex(md5_alg:final())
            end

            if opts.calc_sha1 == true then
                rst.sha1 = resty_string.to_hex(sha1_alg:final())
            end

            break
        end
    end

    return rst
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

        local now = ngx.now()
        local _, err_code, err_msg = http:send_body(data)

        ngx.update_time()
        pobj:incr_stat(ident, "write_size", #data)
        pobj:incr_stat(ident, "write_time", ngx.now()-now)

        bytes = bytes + #data
    end

    return bytes
end

return _M
