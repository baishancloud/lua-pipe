local strutil = require("acid.strutil")
local tableutil = require("acid.tableutil")
local semaphore = require("ngx.semaphore")
local pipe_filter = require("pipe.filter")
local pipe_reader = require("pipe.reader")
local pipe_writer = require("pipe.writer")

local _M = { _VERSION = '1.0' }
local mt = { __index = _M }

_M.reader = pipe_reader
_M.writer = pipe_writer
_M.filter = pipe_filter

local to_str = strutil.to_str
local READ_TIMEOUT = 300 * 1000 --ms
local WRITE_TIMEOUT = 300 * 1000 --ms

local function wrap_co_func(co, ...)
    local ok, rst, err_code, err_msg = pcall(co.func, ...)

    co.sema_buf_ready:post(1)
    co.sema_buf_filled:post(1)
    co.sema_dead:post(1)

    if ok and err_code == nil then
        co.result = rst
    else
        if not ok then
            err_code, err_msg = 'CoroutineError', rst
        end
        co.err = {err_code = err_code, err_msg = err_msg}
        ngx.log(ngx.ERR, to_str(co.rd_or_wrt, " coroutine exit with error:", co.err))
    end

    co.is_dead = true
end

local function spawn_coroutines(functions, rd_or_wrt)
    local cos = {}

    for ident, func in ipairs(functions) do
        if type(func) ~= 'function' then
            return nil, 'InvalidArguments', to_str(rd_or_wrt, ' is not executable')
        end

        table.insert(cos, {
            ident     = ident,
            func      = func,

            result    = nil,
            err       = nil,

            is_dead   = false,
            is_eof    = false,

            rd_or_wrt = rd_or_wrt,

            sema_buf_ready  = semaphore.new(),
            sema_buf_filled = semaphore.new(),
            sema_dead       = semaphore.new(),
        })
    end

    return cos
end

local function start_coroutines(self, ...)
    for _, cos in ipairs({...}) do
        for _, co in ipairs(cos) do
            co.co = ngx.thread.spawn(wrap_co_func, co, self, co.ident)
        end
    end
end

local function kill_coroutines(...)
    for _, cos in ipairs({...}) do
        for _, co in ipairs(cos) do
            ngx.thread.kill(co.co)
            co.is_dead = true
        end
    end
end

local function get_pipe_result(self)
    local rst = {
        read_result = {},
        write_result = {},
    }

    for _, co in ipairs(self.rd_cos) do
        table.insert(rst.read_result, {
            err = co.err,
            result = co.result,
        })
    end

    for _, co in ipairs(self.wrt_cos) do
        table.insert(rst.write_result, {
            err = co.err,
            result = co.result,
        })
    end

    return rst
end

local function set_write_result(self, rst)
    for _, co in ipairs(self.wrt_cos) do
        if type(rst) == 'table' then
            co.result = tableutil.dup(rst, true)
        else
            co.result = rst
        end
    end
end

local function is_all_nil(bufs, n)
    for i = 1, n, 1 do
        if bufs[i] ~= nil then
            return false
        end
    end
    return true
end

local function set_nil(bufs, n)
    for i = 1, n, 1 do
        bufs[i] = nil
    end
end

local function set_nil_on_err(cos, bufs, n)
    for i = 1, n, 1 do
        if cos[i].err ~= nil then
            bufs[i] = nil
        end
    end
end

local function is_read_eof(self)
    for _, co in ipairs(self.rd_cos) do
        if co.is_eof == false then
            return false
        end
    end
    return true
end

local function post_co_sema(self, cos, sema)
    for i, co in ipairs(cos) do
        co[sema]:post(1)
    end
end

local function wait_co_sema(self, cos, sema, timeout, err_code)
    for i, co in ipairs(cos) do
        if not co.is_dead then
            local t0 = ngx.now()
            local ok, err = co[sema]:wait(timeout)
            timeout = math.max(timeout - (ngx.now() - t0), 0.001)
            if err then
                co.err = {
                    err_code = err_code,
                    err_msg  = to_str('pipe wait ',
                        co.rd_or_wrt, ' sempahore ', sema, ' error:', err),
                }
            end
        end
    end
end

local function async_wait_co_sema(self, cos, sema, quorum, timeout, err_code)
    local dead_time = ngx.now() + timeout

    while ngx.now() <= dead_time do
        local n_ok = 0
        local n_active = 0

        for _, co in ipairs(cos) do
            if co.is_dead then
                if co.err == nil then
                    n_ok = n_ok + 1
                end
            else
                n_active = n_active + 1
            end
        end

        if n_ok >= quorum then
            return
        end

        if n_active + n_ok < quorum then
            break
        end

        ngx.sleep(0.001)
    end

    for _, co in ipairs(cos) do
        if not co.is_dead then
            co.err = co.err or {
                err_code = err_code,
                err_msg  = to_str('pipe wait ',
                    co.rd_or_wrt, ' sempahore ', sema, ' timeout'),
            }
        end
    end
end

local function run_filters(self, filters)
    local pipe_rst = get_pipe_result(self)

    for _, filter in ipairs(filters) do
        local rst, err_code, err_msg = filter(self.rbufs, self.n_rd,
            self.wbufs, self.n_wrt, pipe_rst)

        if err_code ~= nil then
            return rst, err_code, err_msg
        end
    end
end

function _M.new(_, rds, wrts, filters, rd_timeout, wrt_timeout)
    if #rds == 0 or #wrts == 0 then
        return nil, 'InvalidArgs', 'reader or writer cant be empty'
    end

    local rd_cos, err_code, err_msg = spawn_coroutines(rds, 'reader')
    if err_code ~= nil then
        return nil, err_code, err_msg
    end

    local wrt_cos, err_code, err_msg = spawn_coroutines(wrts, 'writer')
    if err_code ~= nil then
        return nil, err_code, err_msg
    end

    filters = filters or {}

    local obj = {
        n_rd  = #rds,
        n_wrt = #wrts,

        rd_cos  = rd_cos,
        wrt_cos = wrt_cos,

        rbufs = {},
        wbufs = {},

        rd_filters  = filters.rd_filters or {pipe_filter.copy_filter},
        wrt_filters = filters.wrt_filters
            or {pipe_filter.make_write_quorum_filter(#wrts)},

        rd_timeout = (rd_timeout or READ_TIMEOUT)/1000,
        wrt_timeout = (wrt_timeout or WRITE_TIMEOUT)/1000,
    }

    return setmetatable(obj, mt)
end

function _M.write_pipe(pobj, ident, buf)
    local rd_co = pobj.rd_cos[ident]

    if rd_co.err ~= nil then
        return nil, rd_co.err.err_code, rd_co.err.err_msg
    end

    local ok, err = rd_co.sema_buf_ready:wait(pobj.wrt_timeout)
    if err then
        return nil, 'WriteTimeout',
            to_str('reader ', ident, ' wait buffer ready sempahore error:', err)
    end

    if rd_co.err ~= nil then
        return nil, rd_co.err.err_code, rd_co.err.err_msg
    end

    if buf == '' then
        rd_co.is_eof = true
    end

    pobj.rbufs[ident] = buf

    rd_co.sema_buf_filled:post(1)
end

function _M.read_pipe(pobj, ident)
    local wrt_co = pobj.wrt_cos[ident]

    if wrt_co.err ~= nil then
        return nil, wrt_co.err.err_code, wrt_co.err.err_msg
    end

    local ok, err = wrt_co.sema_buf_filled:wait(pobj.rd_timeout)
    if err then
        return nil, 'ReadTimeout',
            to_str('write ', ident, ' wait buffer filled sempahore error:', err)
    end

    if wrt_co.err ~= nil then
        return nil, wrt_co.err.err_code, wrt_co.err.err_msg
    end

    local buf = pobj.wbufs[ident]
    pobj.wbufs[ident] = nil

    wrt_co.sema_buf_ready:post(1)

    return buf
end

function _M.pipe(self, is_running, quorum_return)
    start_coroutines(self, self.rd_cos, self.wrt_cos)

    while not is_read_eof(self) do
        if not is_running() then
            kill_coroutines(self.rd_cos, self.wrt_cos)
            return nil, 'AbortedError', 'aborted by caller'
        end

        set_nil(self.rbufs, self.n_rd)
        post_co_sema(self, self.rd_cos, 'sema_buf_ready')
        wait_co_sema(self, self.rd_cos,
            'sema_buf_filled', self.rd_timeout, 'ReadTimeout')

        set_nil_on_err(self.rd_cos, self.rbufs, self.n_rd)

        local rst, err_code, err_msg = run_filters(self, self.rd_filters)
        if err_code ~= nil then
            kill_coroutines(self.rd_cos, self.wrt_cos)

            if err_code ~= 'InterruptError' then
                return nil, err_code, err_msg
            end

            set_write_result(self, rst)
            return get_pipe_result(self)
        end

        if is_all_nil(self.wbufs, self.n_wrt) then
            kill_coroutines(self.rd_cos, self.wrt_cos)
            return nil, 'PipeError', 'to write data can not be nil'
        end

        post_co_sema(self, self.wrt_cos, 'sema_buf_filled')
        wait_co_sema(self, self.wrt_cos,
            'sema_buf_ready', self.wrt_timeout, 'WriteTimeout')

        local _, err_code, err_msg = run_filters(self, self.wrt_filters)
        if err_code ~= nil then
            kill_coroutines(self.rd_cos, self.wrt_cos)
            return nil, err_code, err_msg
        end
    end

    if quorum_return ~= nil then
        async_wait_co_sema(self, self.wrt_cos,
            'sema_dead', quorum_return, self.wrt_timeout, 'WriteTimeout')
    else
        wait_co_sema(self, self.wrt_cos,
            'sema_dead', self.wrt_timeout, 'WriteTimeout')
    end
    kill_coroutines(self.rd_cos, self.wrt_cos)

    return get_pipe_result(self)
end

return _M
