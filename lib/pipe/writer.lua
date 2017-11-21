local httplib = require("pipe.httplib")
local rpc_logging = require("acid.rpc_logging")
local acid_setutil = require("acid.setutil")

local _M = { _VERSION = '1.0' }

local INF = math.huge

local function write_data_to_ngx(pobj, ident, opts)
    opts = opts or {}

    -- range = {start, end} is rfc2612 Range header,
    -- a closed interval, starts with index 0
    local range = opts.range
    local pipe_log = opts.pipe_log

    local recv_left, recv_right = 0, 0
    local from, to
    if range ~= nil then
        from = range['start'] + 1

        if range['end'] ~= nil then
            to = range['end'] + 1
        else
            to = INF
        end

        if from > to then
            return nil, 'InvalidRange', string.format(
                'start: %d is greater than end: %d', from, to)
        end
    end

    while true do
        local data, err, err_msg
        if pipe_log ~= nil then
            rpc_logging.reset_start(pipe_log)

            data, err, err_msg = pobj:read_pipe(ident)

            rpc_logging.set_err(pipe_log, err)
            rpc_logging.incr_stat(pipe_log, 'downstream', 'sendbody', #(data or ''))
        else
            data, err, err_msg = pobj:read_pipe(ident)
        end

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

        ngx.print(data)
        local _, err = ngx.flush(true)
        if err then
            return nil, 'ClientAborted', err
        end
    end

    return recv_right
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

return _M
