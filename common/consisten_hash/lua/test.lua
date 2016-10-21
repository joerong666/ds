require("consistent_hash")


print("loading consistent_hash c lib(so)...")
local ver = consistent_hash.version()
print(ver)

key = "a"
ip_list = {}
table.insert(ip_list, "10.1.74.51")
table.insert(ip_list, "10.1.74.52")
table.insert(ip_list, "10.1.74.49")
local cont, msg = consistent_hash.consistent_hash_init(ip_list)
if cont == nil then
    print(msg)
else
    print(consistent_hash.ch_hash(key))
    print(cont:ch_get_server(key))
    local num = cont:ch_get_server_num() - 1
    for i=0, num, 1 do
        print(cont:ch_print_server(i))
    end
    cont:consistent_hash_destory();
end


