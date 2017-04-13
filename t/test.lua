local pipe_pipe = require("pipe.pipe")
local util = require('pipe.t.util')
local resty_md5 = require("resty.md5")
local resty_string = require("resty.string")

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

local function make_calc_md5_filter(rst)
    local md5 = resty_md5:new()
    if not md5 then
        return nil, 'Md5Error', "failed to create md5 object"
    end

    return function(rbufs, n_rd, wbufs, n_wrt, pipe_rst)
        for i = 1, n_wrt, 1 do
            wbufs[i] = rbufs[1]
        end

       if rbufs[1] == nil then
           return nil, 'ReadError', 'read bufs is nil'
       end

       if rbufs[1] == '' then
            local digest = md5:final()
            rst[1] = resty_string.to_hex(digest)
        end

        local ok = md5:update(rbufs[1])
        if not ok then
            return nil, 'Md5Error', 'failed to add data'
        end
    end
end

function _M.test_pipe_http_reader()
    local wrt_files = {
        '/tmp/t1.out',
        '/tmp/t2.out',
        '/tmp/t3.out',
    }

    local writers = {}

    for _, fpath in ipairs(wrt_files) do
        local file_writer = pipe_pipe.writer.make_file_writer(fpath)
        table.insert(writers, file_writer)
    end

    local domain = 'www.lua.org'
    local ips, err_code, err_msg = util.get_ips_from_domain(domain)
    if err_code ~= nil then
        return nil, err_code, err_msg
    end

    local uri = '/ftp/lua-5.3.4.tar.gz'
    local rd_opts = {headers={Host=domain}}

    local http_reader = pipe_pipe.reader.make_http_reader(ips, 80, 'GET',uri, rd_opts)

    local md5_rst = {}
    local md5_filter = make_calc_md5_filter(md5_rst)

    local cpipe, err_code, err_msg = pipe_pipe:new(
        {http_reader}, writers, {rd_filters={md5_filter}}, 30)
    if err_code ~= nil then
        util.rm_files(unpack(wrt_files))
        return nil, err_code, err_msg
    end

    local rst, err_code, err_msg = cpipe:pipe(is_running)
    if err_code ~= nil then
        util.rm_files(unpack(wrt_files))
        return rst, err_code, err_msg
    end

    for _, fpath in ipairs( wrt_files ) do
        local t1_md5_val, err_code, err_msg = util.get_file_md5(fpath)
        if err_code ~= nil then
            util.rm_files(unpack(wrt_files))
            return nil, err_code, err_msg
        end

        if t1_md5_val ~= md5_rst[1] then
            util.rm_files(unpack(wrt_files))
            return nil, 'Md5Error', 'not equal'
        end
    end

    util.rm_files(unpack(wrt_files))
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


function _M.test_pipe_file_reader_buffer_writer()
    local data_src = '/dev/urandom'

    local fp, err_msg = io.open(data_src, 'r')
    if fp == nil then
        return nil, 'FileError', err_msg
    end

    local data = fp:read(1024 + 512)
    if data == nil then
        fp:close()
        return nil, 'FileError', 'can not read data'
    end

    fp:close()

    local fpath = string.format('/tmp/%d', ngx.time())
    fp, err_msg = io.open(fpath, 'w')
    if fp == nil then
        return nil, 'FileError', err_msg
    end

    fp:write(data)
    fp:close()

    local buffer = {}
    local reader = pipe_pipe.reader.make_file_reader(fpath, 1024)
    local writer = pipe_pipe.writer.make_buffer_writer(buffer)

    local cpipe, err_code, err_msg = pipe_pipe:new({reader}, {writer})
    if err_code ~= nil then
        os.remove(fpath)
        return nil, err_code, err_msg
    end

    local rst, err_code, err_msg = cpipe:pipe(is_running)
    if err_code ~= nil then
        os.remove(fpath)
        return rst, err_code, err_msg
    end

    os.remove(fpath)

    local data_sha1 = ngx.sha1_bin(data)
    local pipe_sha1 = ngx.sha1_bin(buffer.buf)

    if data_sha1 ~= pipe_sha1 then
        return nil, 'ReadWriteError', 'data not the same'
    end

    ngx.log(ngx.INFO, 'data_sha1 and pipe_sha1 equals')
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

    ngx.eof()
    ngx.exit(ngx.HTTP_OK)
end


return _M
