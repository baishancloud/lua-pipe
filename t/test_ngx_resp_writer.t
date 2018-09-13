# vim:set ft=lua ts=4 sw=4 et ft=perl:

use Test::Nginx::Socket "no_plan";

no_long_string();

run_tests();

__DATA__

=== TEST 1: test make_ngx_resp_writer
--- http_config
    lua_check_client_abort on;

    lua_package_path "./lib/?.lua;;";
    lua_package_cpath "./lib/?.so;;";
--- config
    location /t {
        content_by_lua '
            local pipe_pipe = require("pipe.pipe")

            local function is_running()
                return true
            end

            local read_datas = {"1", "23", "4", "5", "678", "9", "0"}
            local readers = {pipe_pipe.reader.make_memery_reader(read_datas)}

            local status = 200
            local headers = {["Content-Length"]=3}
            local opts = {range ={}}
            opts.range["from"] = 2
            opts.range["to"] = 4

            local writers = {pipe_pipe.writer.make_ngx_resp_writer(status, headers, opts)}

            local cpipe, err, err_msg = pipe_pipe:new(readers, writers)
            if err ~= nil then
                ngx.log(ngx.ERR, string.format("err: %s, err_msg: %s", err, err_msg))
                return
            end

            local _, err, err_msg = cpipe:pipe(is_running)
            if err ~= nil then
                ngx.log(ngx.ERR, string.format("err: %s, err_msg: %s", err, err_msg))
                return
            end
        ';
    }
--- request
GET /t
--- response_body: 345
--- no_error_log
[error]
