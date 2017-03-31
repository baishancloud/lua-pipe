local _M = {}

local resty_md5 = require("resty.md5")
local resty_string = require("resty.string")
local resolver = require( "resty.dns.resolver" )

local NAME_SERVERS = { "113.114.114.114", "119.29.29.29", "10.202.72.118" }

function _M.rm_files(...)
    for _, fpath in ipairs({...}) do
        os.remove(fpath)
    end
end

function _M.get_file_md5(fpath)
    local md5 = resty_md5:new()
    if not md5 then
        return nil, 'Md5Error', "failed to create md object"
    end

    local fp, err_msg = io.open(fpath, 'r')
    if fp == nil then
        return nil, 'FileError', err_msg
    end

    while true do
        local data, err = fp:read(1024*1024)
        if err ~= nil then
            fp:close()
            return nil, 'FileError', 'read data error, file path: '..fpath
        end

        if data == nil then
            break
        end

        local ok = md5:update(data)
        if not ok then
            return nil, 'Md5Error', "failed to add data"
        end
    end

    fp:close()

    return resty_string.to_hex(md5:final())
end

function _M.resolve_doman( dname )

    local res, answers, err_msg

    res, err_msg = resolver:new{
        nameservers = NAME_SERVERS,
        retrans = 5,
        timeout = 2000,
    }
    if err_msg then
        return nil, 'ResolverError', err_msg .. dname
    end

    answers, err_msg = res:query( dname )
    if err_msg then
        return nil, 'ResolveError', err_msg
    end

    if answers.errcode then
        return nil, 'ResolverError', answers.errstr
    end

    return answers, nil, nil
end

function _M.get_ips_from_domain( domain )
    local answers, err_code, err_msg = _M.resolve_doman(domain)
    if err_code ~= nil then
        return nil, err_code, err_msg
    end

    local ips = {}
    for _, ans in ipairs( answers ) do
        if ans.type == 1 and ans.address then
            table.insert( ips, ans.address )
        end
    end
    if #ips == 0 then
        return nil, 'ResolverError', "has no ip for domain:" .. domain
    end

    return ips
end

return _M
