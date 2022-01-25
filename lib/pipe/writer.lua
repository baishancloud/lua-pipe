local httplib = require("pipe.httplib")
local strutil = require("acid.strutil")
local tableutil = require("acid.tableutil")
local rpc_logging = require("acid.rpc_logging")
local acid_setutil = require("acid.setutil")
local s3_client = require('resty.aws_s3.client')
local hashlib = require("acid.hashlib")
local resty_string = require("resty.string")
local aws_chunk_writer = require("resty.aws_chunk.writer")

local _M = { _VERSION = '1.0' }

local to_str = strutil.to_str
local INF = math.huge

local function write_data_to_ngx(pobj, ident, opts)
    opts = opts or {}

    -- range = {from, to} is rfc2612 Range header,
    -- a closed interval, starts with index 0
    local range = opts.range

    local log = rpc_logging.new_entry('write_client')
    rpc_logging.add_log(log)

    local ret = {
            size = 0,
        }

    local recv_left, recv_right = 0, 0
    local from, to
    if range ~= nil then
        from = range['from'] + 1

        if range['to'] ~= nil then
            to = range['to'] + 1
        else
            to = INF
        end

        if from > to then
            return nil, 'InvalidRange', string.format(
                'from: %d is greater than to: %d', from, to)
        end
    end

    local alg_sha1 = nil

    if opts.body_sha1 ~= nil then
        alg_sha1 = hashlib:sha1()
    end

    while true do
        rpc_logging.reset_start(log)

        local data, err, err_msg = pobj:read_pipe(ident)

        rpc_logging.set_err(log, err)
        rpc_logging.incr_stat(log, 'upstream', 'recvbody', #(data or ''))

        if err ~= nil then
            return nil, err, err_msg
        end

        if data == '' then
            break
        end

        recv_left = recv_right + 1
        recv_right = recv_right + #data

        if from ~= nil then
            local intersection, err, err_msg =
                acid_setutil.intersect(from, to, recv_left, recv_right)
            if err ~= nil then
                return nil, err, err_msg
            end

            local f, t = intersection.from, intersection.to
            if f == nil then
                data = ''
            else
                if t - f + 1 ~= #data then
                    data = data:sub(f - recv_left + 1, t - recv_left + 1)
                end
            end
        end

        ret.size = ret.size + #data

        if alg_sha1 ~= nil then
            alg_sha1:update(data)
        end

        if ret.size == opts.total_size then
            if alg_sha1 ~= nil then
                local calc_sha1 = resty_string.to_hex(alg_sha1:final())
                if calc_sha1 ~= opts.body_sha1 then
                    return nil, "Sha1Notmatched", to_str("expect:", opts.body_sha1, ", actual:", calc_sha1)
                end
            end
        end

        if opts.bandwidth_cb ~= nil then
            opts.bandwidth_cb(#data)
        end

        rpc_logging.reset_start(log)

        ngx.print(data)
        local _, err = ngx.flush(true)

        rpc_logging.set_err(log, err)
        rpc_logging.incr_stat(log, 'downstream', 'sendbody', #(data or ''))
        if err then
            return nil, 'ClientAborted', err
        end
    end

    return ret
end

function _M.connect_http(ips, port, verb, uri, opts)
    return httplib.connect_http(ips, port, verb, uri, opts)
end

function _M.loop_http_write(pobj, ident, http)
    return httplib.loop_http_write(pobj, ident, http)
end

function _M.get_http_response(http, opts)
    return httplib.get_http_response(http, opts)
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

function _M.make_connected_http_writer(http, opts)
    return function(pobj, ident)
        local _, err_code, err_msg = _M.loop_http_write(pobj, ident, http)
        if err_code ~= nil then
            return nil, err_code, err_msg
        end

        return _M.get_http_response(http, opts)
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

        return _M.get_http_response(http, opts)
    end
end

function _M.make_ngx_writer(opts)
    return function(pobj, ident)
        return write_data_to_ngx(pobj, ident, opts)
    end
end


function _M.make_ngx_resp_writer(status, headers, opts)
    ngx.status = status
    for k, v in pairs(headers) do
        ngx.header[k] = v
    end

    return function(pobj, ident)
        return write_data_to_ngx(pobj, ident, opts)
    end
end


function _M.make_buffer_writer(buffer, do_concat)
    do_concat = (do_concat ~= false)
    if buffer == nil then
        return nil, 'InvalidArgument', 'buffer is nil'
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

function _M.make_quorum_http_writers(dests, writer_opts, quorum)
    local conn_threads = {}

    for _, dest in ipairs(dests) do
        local th = ngx.thread.spawn(_M.connect_http,
            dest.ips, dest.port, dest.method, dest.uri, writer_opts)
        table.insert(conn_threads, th)
    end

    local writers = {}
    local n_ok = 0
    for _, th in ipairs(conn_threads) do
        local wrt = {}

        local ok, http, err_code, err_msg = ngx.thread.wait(th)
        if ok and err_code == nil then
            n_ok = n_ok + 1
            wrt.http = http
            wrt.writer = _M.make_connected_http_writer(http, writer_opts)
        else
            wrt.err = {
                err_code = err_code or 'CoroutineError',
                err_msg = err_msg or 'coroutine error, when connect',
            }
        end
        table.insert(writers, wrt)
    end

    if n_ok >= quorum then
        return writers
    end

    for _, wrt in ipairs(writers) do
        if wrt.http ~= nil then
            wrt.http:close()
        end
        wrt.http = nil
    end

    return nil, 'NotEnoughConnect', to_str('quorum:', quorum, ", actual:", n_ok)
end

function _M.make_put_s3_writer(access_key, secret_key, endpoint, params, opts)
    local s3_cli, err_code, err_msg =
        s3_client.new(access_key, secret_key, endpoint, opts)
    if err_code ~= nil then
        return nil, err_code, err_msg
    end

    local request, err_code, err_msg =
        s3_cli:get_signed_request(params, 'put_object', opts)
    if err_code ~= nil then
        return nil, err_code, err_msg
    end

    return function(pobj, ident)
        local chunk_writer
        if opts.aws_chunk == true then
            chunk_writer =
                aws_chunk_writer:new(request.signer, request.auth_ctx)
        end

        local _, err_code, err_msg = s3_cli:send_request(
            request.verb, request.uri, request.headers,request.body)
        if err_code ~= nil then
            return nil, err_code, err_msg
        end

        while true do
            local data, err_code, err_msg = pobj:read_pipe(ident)
            if err_code ~= nil then
                return nil, err_code, err_msg
            end

            local send_data = data
            if opts.aws_chunk == true then
                send_data = chunk_writer:make_chunk(send_data)
            end

            local _, err_code, err_msg = s3_cli:send_body(send_data)
            if err_code ~= nil then
                return nil, err_code, err_msg
            end

            if data == '' then
                break
            end
        end

        return s3_cli:finish_request()
    end
end

function _M.make_aws_put_s3_writer(access_key, secret_key, endpoint, params, opts)
    opts = tableutil.dup(opts or {}, true)
    opts.aws_chunk = true

    return _M.make_put_s3_writer(access_key, secret_key, endpoint, params, opts)
end

return _M
