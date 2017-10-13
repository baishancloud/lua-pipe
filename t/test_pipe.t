# vim:set ft=lua ts=4 sw=4 et ft=perl:

use Test::Nginx::Socket "no_plan";

no_long_string();

run_tests();

__DATA__

=== TEST 1: test pipe
--- http_config
    lua_check_client_abort on;

    lua_package_path "./lib/?.lua;;";
    lua_package_cpath "./lib/?.so;;";
--- config
    location /t {
        content_by_lua_block {
            require("t.test").test()
        }
    }
--- request
GET /t
--- response_body_like
.*tests all passed.*
--- timeout: 30
