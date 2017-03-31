local pipe_pipe = require("pipe.pipe")
local _M = {}

local is_running = function()
    return true
end

local function memery_writer(pobj, ident)
    while true do
        local data, err_code, err_msg = pobj:read_pipe(ident)
        if err_code ~= nil then
            return nil, err_code, err_msg
        end

        if data == '' then
            break
        end
    end
end

local function make_memery_reader(datas)
    if type(datas) == type('') then
        datas = {datas}
    end

    table.insert(datas, '')

    return function(pobj, ident)
        for _, data in ipairs(datas) do
            local _, err_code, err_msg = pobj:write_pipe(ident, data)
            if err_code ~= nil then
                return nil, err_code, err_msg
            end

            if data == '' then
                break
            end
        end
    end
end

local function make_check_err_filter(r_or_w, ident, expect_err, return_err)
    return function(rbufs, n_rds, wbufs, n_wrts, pipe_rst)
        local err

        if r_or_w == 'r' then
            err = pipe_rst.read_result[ident].err
        elseif r_or_w == 'w' then
            err = pipe_rst.write_reslut[ident].err
        end

        local err_code = (err or {}).err_code

        if err_code == expect_err then
            return nil, return_err, 'got expected err'
        else
            return nil, 'TestError',
                'expected: ' .. tostring(expect_err) .. ', actual:'.. tostring(err_code)
        end
    end
end

function _M.test_pipe_args()
    local reader_case = {
            {make_memery_reader('123'), nil},
            {'notdunction', 'InvalidArgs'},
            {1234,          'InvalidArgs'}
        }

    for _, case in ipairs(reader_case) do
        local _, err_code, err_msg = pipe_pipe:new({case[1]}, {memery_writer})
        if err_code ~= case[2] then
            return nil, err_code, err_msg
        end
    end
end

function _M.test_pipe_empty_reader()
    local empty_reader = function(pobj, ident)
            while true do
                ngx.sleep(1)
            end
        end

    local check_filter = make_check_err_filter('r', 1, 'SemaphoreError', 'TestSuccess')

    local cpipe, err_code, err_msg = pipe_pipe:new({empty_reader},
         {memery_writer}, {rd_filters = {check_filter}}, 2)
    if err_code ~= nil then
        return nil, err_code, err_msg
    end

    local rst, err_code, err_msg = cpipe:pipe(is_running)
    if err_code ~= 'TestSuccess' then
        return nil, 'TestEmptyReaderError', tostring(err_code) .. ':'.. tostring(err_msg)
    end
end

function _M.test_pipe_not_eof_reader()
    local not_eof_reader =
        function(pobj, ident) return pobj:write(ident, '123') end

    local cpipe, err_code, err_msg =
        pipe_pipe:new({not_eof_reader}, {memery_writer})
    if err_code ~= nil then
        return nil, err_code, err_msg
    end

    local rst, err_code, err_msg = cpipe:pipe(is_running)
    if err_code ~= 'PipeError' then
        return nil, 'TestNotEOFReaderError', tostring(err_code) .. ':'.. tostring(err_msg)
    end
end

function _M.test_pipe_error_reader()
    local err_reader =
        function(pobj, ident) return pobj:wr(ident, '123') end

    local check_filter = make_check_err_filter('r', 1, 'CoroutineError', 'TestSuccess')

    local cpipe, err_code, err_msg = pipe_pipe:new({err_reader},
         {memery_writer}, {rd_filters = {check_filter}})
    if err_code ~= nil then
        return nil, err_code, err_msg
    end

    local rst, err_code, err_msg = cpipe:pipe(is_running)
    if err_code ~= 'TestSuccess' then
        return nil, 'TestErrReaderError', tostring(err_code) .. ':'.. tostring(err_msg)
    end
end

function _M.test_pipe_interrupt()
    local test_datas = {'xxx', 'yyy'}
    local test_interrput_rst = 'zzz'

    local interrupt_filter =
        function(rbufs, n_rds, wbufs, n_wrts, pipe_rst)
        if rbufs[1] == 'yyy' then
            return test_interrput_rst, 'InterruptError', ''
        end
        wbufs[1] = rbufs[1]
    end

    local readers = {make_memery_reader(test_datas)}
    local writers = {memery_writer}
    local filters = {rd_filters = {interrupt_filter}}

    local cpipe, err_code, err_msg = pipe_pipe:new(readers, writers, filters)
    if err_code ~= nil then
        return nil, err_code, err_msg
    end

    local rst, err_code, err_msg = cpipe:pipe(is_running)
    if err_code ~= nil
         or rst.write_result[1].result ~= test_interrput_rst then
        return nil, 'TestInterruptError', tostring(err_code) .. ':'.. tostring(err_msg)
    end
end

function _M.test_pipe_abort()
    local read_datas = {'xxx', 'yyy', 'zzz'}

    local times = 0
    local read_times_filter =
        function(rbufs, n_rds, wbufs, n_wrts, pipe_rst)
            times = times + 1
        end

    local is_running = function()
        if times > 1 then
            return false
        else
            return true
        end
    end

    local cpipe, err_code, err_msg = pipe_pipe:new({make_memery_reader(read_datas)},
         {memery_writer}, {rd_filters = {read_times_filter, pipe_pipe.filter.copy_filter}})
    if err_code ~= nil then
        return nil, err_code, err_msg
    end

    local _, err_code, err_msg = cpipe:pipe(is_running)
    if err_code ~= 'AbortedError' then
        return nil, 'TestAbortError', tostring(err_code) .. ':'.. tostring(err_msg)
    end
end

function _M.test_pipe_not_enough_quorum()
    local read_datas = {'xxx', 'yyy', 'zzz'}

    local err_writer = function(pobj, ident)
            return nil, 'writeError', ''
        end

    local cpipe, err_code, err_msg = pipe_pipe:new(
        {make_memery_reader(read_datas)}, {memery_writer, err_writer})
    if err_code ~= nil then
        return nil, err_code, err_msg
    end

    local _, err_code, err_msg = cpipe:pipe(is_running)
    if err_code ~= 'NotEnoughQuorum' then
        return nil, 'TestNotEnoughQuorum', tostring(err_code) .. ':'.. tostring(err_msg)
    end
end

function _M.test()
    local test_prefix = 'test_pipe_'

    for name, case in pairs(_M) do
        if type(case) == 'function'
            and string.sub(name, 1, #test_prefix) == test_prefix then

            local _ , err_code, err_msg = case()

            assert(err_code == nil,
                'runc test, case: ' .. name ..
                ' err_code: ' ..(err_code or '')..', err_msg:'..(err_msg or ''))
        end
    end
end

return _M
