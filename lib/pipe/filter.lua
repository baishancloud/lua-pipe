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

return _M
