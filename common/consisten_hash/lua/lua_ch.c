#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "lua.h"
#include "lauxlib.h"

#include "ch.h"
#include "log.h"

#define CH_VERSION                       "1.0.0.0"
#define TNAME_CONSISTEN_HASH             "\"consisten_hash\" userdata"

typedef struct ch_lua_if_data
{
    void *ptr;
    int closed;
} ch_lua_if_data_t;


int lch_create_lua_userdata(lua_State *L, void *ptr, const char *tname)
{
    if (!L || !ptr || !tname)
    {
        return -1;
    }

    ch_lua_if_data_t *us = (ch_lua_if_data_t *)lua_newuserdata(L, sizeof(ch_lua_if_data_t));
    if (us == 0)
    {
        return 1;
    }

    luaL_getmetatable(L, tname);
    lua_setmetatable(L, -2);
    us->closed = 0;
    us->ptr = ptr;
    return 0;    
}

void *lch_get_ptr_from_userdata(lua_State *L, int idx, const char *tname)
{
    if (!L || !tname)
    {
        return 0;
    }

    ch_lua_if_data_t *us = (ch_lua_if_data_t *)luaL_checkudata(L, idx, tname);

    char buf[128];
    snprintf(buf, sizeof(buf), "\"%s\" userdata is null", tname);
    luaL_argcheck(L, us != NULL && us->ptr != NULL, idx, buf);

    return us;
}



/* consisten hash */

static void free_keys(char **keys, int size)
{
    int i;
    for (i = 0; i < size; i++)
    {
        if (keys[i])
        {
            free(keys[i]);
            keys[i] = 0;
        }
    }
    free(keys);
}

static int lconsistent_hash_init(lua_State *L)
{
    log_debug("lconsistent_hash_init()...");

    if (lua_istable(L, -1) != 1)
    {
        lua_pushnil(L);
        lua_pushstring(L, "argument #1 is not table");
        return 2;
    }
    
    int i = 0;
    /*
    while (1)
    {
        lua_pushnumber(L, i + 1);
        lua_gettable(L, -2);
        if (lua_isnil(L, -1) == 1)
        {
            lua_pop(L, 1);
            break;
        }
        lua_pop(L, 1);
        i++;
    }
    */
    const int n = lua_objlen(L, -1);

    char **server_keys = 0;
    const char *param = 0;
    size_t param_len;
    server_keys = (char **)calloc(n, sizeof(char *));
    for (i = 0; i < n; i++)
    {
        lua_pushnumber(L, i + 1);
        lua_gettable(L, -2);
        if (lua_isstring(L, -1) != 1)
        {
            free_keys(server_keys, n);
            server_keys = 0;

            lua_pushnil(L);
            lua_pushfstring(L, "No.%d field in argument #1 isn't string", i + 1);
            return 2;
        }

        param = lua_tolstring(L, -1, &param_len);
        server_keys[i] = (char *)malloc(param_len + 1);
        if (server_keys[i] == 0)
        {
            free_keys(server_keys, n);
            server_keys = 0;

            lua_pushnil(L);
            lua_pushfstring(L, "No.%d malloc() fail for %d size", i + 1, param_len);
            return 2;
        }
        strcpy(server_keys[i], param);
        lua_pop(L, 1);
    }

    serv_node_info_t *cont = consistent_hash_init((const char *const*)server_keys, n);
    free_keys(server_keys, n);
    server_keys = 0;
    if (cont == 0)
    {
        lua_pushnil(L);
        lua_pushstring(L, "consisten_hash_init() fail");
        return 2;
    }

    if (lch_create_lua_userdata(L, cont, TNAME_CONSISTEN_HASH) == 0)
    {
        return 1;
    }
    else
    {
        consistent_hash_destory(cont);

        lua_pushnil(L);
        lua_pushstring(L, "create_lua_userdata fail");
        return 2;
    }
}

static int lch_hash(lua_State *L)
{
    log_debug("lch_hash()...");

    size_t key_len = 0;
    const char *key = luaL_checklstring(L, 1, &key_len);
    if (key == 0 || key_len <= 0)
    {
        lua_pushnil(L);
        lua_pushstring(L, "argument #1 is null pointer");
        return 2;
    }
    
    uint64_t h = ch_hash(key, key_len);
    lua_pushnumber(L, h);

    return 1;
}

static int lch_get_server(lua_State *L)
{
    log_debug("lch_get_server()...");

    ch_lua_if_data_t *us = (ch_lua_if_data_t *)lch_get_ptr_from_userdata(L, 1, TNAME_CONSISTEN_HASH);
    serv_node_info_t *cont = (serv_node_info_t *)us->ptr;
    if (cont == 0)
    {
        lua_pushnil(L);
        lua_pushstring(L, "argument #1 is null pointer");
        return 2;
    }

    size_t key_len = 0;
    const char *key = luaL_checklstring(L, 2, &key_len);
    if (key == 0 || key_len <= 0)
    {
        lua_pushnil(L);
        lua_pushstring(L, "argument #2 is null pointer");
        return 2;
    }
    
    serv_node_t *serv = ch_get_server(cont, key, key_len);
    if (serv)
    {
        lua_pushstring(L, serv->server_key);
        lua_pushnumber(L, serv->point);
        //lua_pushnumber(L, serv->end);
        lua_pushnumber(L, 0);
        return 3;
    }
    else
    {
        lua_pushnil(L);
        lua_pushstring(L, "ch_get_server() fail");
        return 2;
    }
}

static int lch_get_server_num(lua_State *L)
{
    log_debug("lch_get_server_num()...");

    ch_lua_if_data_t *us = (ch_lua_if_data_t *)lch_get_ptr_from_userdata(L, 1, TNAME_CONSISTEN_HASH);
    serv_node_info_t *cont = (serv_node_info_t *)us->ptr;
    if (cont == 0)
    {
        lua_pushnil(L);
        lua_pushstring(L, "argument #1 is null pointer");
        return 2;
    }

    lua_pushnumber(L, cont->numpoints);
    return 1;
}

static int lch_print_server(lua_State *L)
{
    log_debug("lch_print_server()...");

    ch_lua_if_data_t *us = (ch_lua_if_data_t *)lch_get_ptr_from_userdata(L, 1, TNAME_CONSISTEN_HASH);
    serv_node_info_t *cont = (serv_node_info_t *)us->ptr;
    if (cont == 0)
    {
        lua_pushnil(L);
        lua_pushstring(L, "argument #1 is null pointer");
        return 2;
    }

    const int idx = luaL_checknumber(L, 2);
    if (idx < 0 || idx >= cont->numpoints)
    {
        lua_pushnil(L);
        lua_pushstring(L, "argument #2 is illegal");
        return 2;
    }
    
    serv_node_t *serv = cont->servers + idx;
    if (serv)
    {
        lua_pushstring(L, serv->server_key);
        lua_pushnumber(L, serv->point);
        //lua_pushnumber(L, serv->end);
        lua_pushnumber(L, 0);
        return 3;
    }
    else
    {
        lua_pushnil(L);
        lua_pushstring(L, "system fail");
        return 2;
    }
}

static int lconsistent_hash_destory(lua_State *L)
{
    log_debug("lconsistent_hash_destory()...");

    ch_lua_if_data_t *us = (ch_lua_if_data_t *)lch_get_ptr_from_userdata(L, 1, TNAME_CONSISTEN_HASH);
    serv_node_info_t *cont = (serv_node_info_t *)us->ptr;
    if (us->closed || cont == 0)
    {
        lua_pushnil(L);
        lua_pushstring(L, "closed");
        return 2;
    }

    consistent_hash_destory(cont);
    us->closed = 1;

    lua_pushnumber(L, 0);
    return 1;
}

static int lch_gc(lua_State *L)
{
    log_debug("lch_gc()...");

    ch_lua_if_data_t *us = (ch_lua_if_data_t *)lch_get_ptr_from_userdata(L, 1, TNAME_CONSISTEN_HASH);
    serv_node_info_t *cont = (serv_node_info_t *)us->ptr;
    if (us->closed || cont == 0)
    {
        //lua_pushnil(L);
        //lua_pushstring(L, "argument #1 is null pointer");
        return 0;
    }

    consistent_hash_destory(cont);
    us->closed = 1;

    return 0;
}

static int lch_version(lua_State *L)
{
    lua_pushstring(L, CH_VERSION);
    return 1;
}

static const luaL_reg sni_funcs[] =
{
    {"__gc", lch_gc},
    {"ch_get_server", lch_get_server},
    {"ch_get_server_num", lch_get_server_num},
    {"ch_print_server", lch_print_server},
    {"consistent_hash_destory", lconsistent_hash_destory},
    {NULL, NULL}
};

static const luaL_reg consistent_hash_func[] =
{
    {"consistent_hash_init", lconsistent_hash_init},
    {"ch_hash", lch_hash},
    {"version", lch_version},
    {NULL, NULL}
};

static int luchas_tostring (lua_State *L)
{
    char buff[64];
    void *obj = (void *)lua_touserdata (L, 1);

    sprintf (buff, "UCHAS userdata: [%p]", (void *)obj);

    lua_pushfstring (L, "%s (%s)", lua_tostring(L,lua_upvalueindex(1)), buff);
    return 1;
}

static void luchas_createmeta (lua_State *L, const char *name, const luaL_Reg *methods)
{
    luaL_newmetatable (L, name);

    /* define methods */
    luaL_openlib (L, NULL, methods, 0);

    /* define metamethods */
    lua_pushliteral (L, "__index");
    lua_pushvalue (L, -2);
    lua_settable (L, -3);

    lua_pushliteral (L, "__tostring");
    lua_pushstring (L, name);
    lua_pushcclosure (L, luchas_tostring, 1);
    lua_settable (L, -3);

    lua_pushliteral (L, "__metatable");
    lua_pushliteral (L, "LuaUCHAS: you're not allowed to get this metatable");
    lua_settable (L, -3);

    lua_pop(L, 1);
}

extern int luaopen_consistent_hash(lua_State *L)
{
    luchas_createmeta(L, TNAME_CONSISTEN_HASH, sni_funcs);

    luaL_register(L, "consistent_hash", consistent_hash_func);

    return 1;
}

