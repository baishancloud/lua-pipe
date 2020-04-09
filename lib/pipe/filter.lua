local strutil = require("acid.strutil")

local _M = { _VERSION = '1.0' }

local to_str = strutil.to_str

function _M.copy_filter(rbufs, n_rd, wbufs, n_wrt, pipe_rst)
    for i = 1, n_wrt, 1 do
        wbufs[i] = rbufs[1]
    end
end

function _M.make_rbufs_not_nil_filter(r_idx)
    return function(rbufs, n_rd, wbufs, n_wrt, pipe_rst)
        if rbufs[r_idx] == nil then
            local err = pipe_rst.read_result[r_idx].err
            if err == nil then
                err = {
                    err_code = 'ReadDataError',
                    err_msg  = 'read buf is nil',
                }
            end
            return nil, err.err_code, err.err_msg
        end
    end
end

function _M.make_write_quorum_filter(quorum)
    return function(rbufs, n_rd, wbufs, n_wrt, pipe_rst)
        local n_ok = 0
        for _, wrt_rst in ipairs(pipe_rst.write_result) do
            if wrt_rst.err == nil then
                n_ok = n_ok + 1
            end
        end

        if n_ok < quorum then
            return nil, 'NotEnoughQuorum', to_str('quorum:', quorum, ", actual:", n_ok)
        end
    end
end

function _M.make_read_timeout_filter(r_idx)
    return function(rbufs, n_rd, wbufs, n_wrt, pipe_rst)
        local co_err = pipe_rst.read_result[r_idx].err or {}
        if co_err.err_code == 'ReadTimeout' then
            return nil, co_err.err_code, co_err.err_msg
        end
    end
end

function _M.make_read_max_size_filter(max_size, r_idx)
    local size = 0

    return function(rbufs, n_rd, wbufs, n_wrt, pipe_rst)
        size = size + #(rbufs[r_idx] or '')
        if size > max_size then
            return nil, 'EntityTooLarge',
                string.format('read size %s large than %s', size, max_size)
        end
    end
end

function _M.make_kill_low_write_speed_filter(pobj, assert_func, quorum)
    local all_stat = pobj:get_stat()

    return function(rbufs, n_rd, wbufs, n_wrt, pipe_rst)
        local ok_stat, n_ok = {}, 0
        for idx, wrt_rst in pairs(pipe_rst.write_result) do
            if wrt_rst.err == nil then
                local ident = pobj.wrt_cos[idx].ident
                local id_stat = all_stat[ident] or {}

                if id_stat.write_time ~= nil and id_stat.write_size ~= nil then
                    ok_stat[ident] = {
                       write_size = id_stat.write_size,
                       write_time = id_stat.write_time,
                   }
                   n_ok = n_ok + 1
               end
           end
       end

       if n_ok <= quorum then
           return nil, nil, nil
       end

       for ident, st in pairs(ok_stat) do
            local cur_speed = st.write_size/(math.max(st.write_time * 1000, 1)/1000)

            if assert_func(ok_stat, ident, st, cur_speed) then
                local err = {
                    err_code = "WriteSlow",
                    err_msg = to_str(ident, " coroutine write slow, speed:",
                        strutil.placeholder(cur_speed/1024, '-', '%.3f'), "kb/s"),
                }

                pobj.wrt_cos[ident].err = err
                ngx.log(ngx.ERR, to_str("slow coroutine:", pobj.wrt_cos[ident], ", error:", err))

                break
            end
        end
    end
end

return _M
