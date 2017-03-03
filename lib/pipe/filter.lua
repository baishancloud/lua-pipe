local strutil = require("acid.strutil")

local _M = { _VERSION = '1.0' }
local mt = { __index = _M }

local to_str = strutil.to_str

function _M.copy_filter(rbufs, n_rd, wbufs, n_wrt, pipe_rst)
    for i = 1, n_wrt, 1 do
        wbufs[i] = rbufs[1]
    end

    local n_ok = 0
    for _, rst in ipairs(pipe_rst.write_result) do
        if rst.err == nil then
            n_ok = n_ok + 1
        end
    end

    if n_ok ~= n_wrt then
        return false, 'NotEnoughQuorum', to_str('quorum:', n_wrt, ", actual:", n_ok)
    end
end

return _M
