#include "redis.h"
#include "heartbeat.h"
#include "key_filter.h"
#include "zmalloc.h"

/*-----------------------------------------------------------------------------
 * Config file parsing
 *----------------------------------------------------------------------------*/

int yesnotoi(char *s) {
    if (!strcasecmp(s,"yes")) return 1;
    else if (!strcasecmp(s,"no")) return 0;
    else return -1;
}

void appendServerSaveParams(time_t seconds, int changes) {
    server.saveparams = zrealloc(server.saveparams,sizeof(struct saveparam)*(server.saveparamslen+1));
    server.saveparams[server.saveparamslen].seconds = seconds;
    server.saveparams[server.saveparamslen].changes = changes;
    server.saveparamslen++;
}

void resetServerSaveParams() {
    zfree(server.saveparams);
    server.saveparams = NULL;
    server.saveparamslen = 0;
}

/* I agree, this is a very rudimental way to load a configuration...
   will improve later if the config gets more complex */
void loadServerConfig(char *filename)
{
    FILE *fp;
    char buf[REDIS_CONFIGLINE_MAX+1], *err = NULL;
    int linenum = 0;
    sds line = NULL;

    if (filename[0] == '-' && filename[1] == '\0')
        fp = stdin;
    else {
        if ((fp = fopen(filename,"r")) == NULL) {
            redisLog(REDIS_WARNING, "Fatal error, can't open config file '%s'", filename);
            exit(1);
        }
    }

    while(fgets(buf,REDIS_CONFIGLINE_MAX+1,fp) != NULL) {
        sds *argv;
        int argc, j;

        linenum++;
        line = sdsnew(buf);
        line = sdstrim(line," \t\r\n");

        /* Skip comments and blank lines*/
        if (line[0] == '#' || line[0] == '\0') {
            sdsfree(line);
            continue;
        }

        /* Split into arguments */
        argv = sdssplitargs(line,&argc);
        sdstolower(argv[0]);

        /* Execute config directives */
        if (!strcasecmp(argv[0],"timeout") && argc == 2) {
            server.maxidletime = atoi(argv[1]);
            if (server.maxidletime < 0) {
                err = "Invalid timeout value"; goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"port") && argc == 2) {
            server.port = atoi(argv[1]);
            if (server.port < 0 || server.port > 65535) {
                err = "Invalid port"; goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"bind") && argc == 2) {
            server.bindaddr = zstrdup(argv[1]);
        } else if (!strcasecmp(argv[0],"unixsocket") && argc == 2) {
            server.unixsocket = zstrdup(argv[1]);
        } else if (!strcasecmp(argv[0],"unixsocketperm") && argc == 2) {
            errno = 0;
            server.unixsocketperm = (mode_t)strtol(argv[1], NULL, 8);
            if (errno || server.unixsocketperm > 0777) {
                err = "Invalid socket file permissions"; goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"loglevel") && argc == 2) {
            if (!strcasecmp(argv[1],"debug")) server.verbosity = REDIS_DEBUG;
            else if (!strcasecmp(argv[1],"verbose")) server.verbosity = REDIS_VERBOSE;
            else if (!strcasecmp(argv[1],"notice")) server.verbosity = REDIS_NOTICE;
            else if (!strcasecmp(argv[1],"warning")) server.verbosity = REDIS_WARNING;
            else {
                err = "Invalid log level. Must be one of debug, verbose, notice, warning";
                goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"log_dir") && argc == 2) {
            if (server.log_dir)
            {
                zfree(server.log_dir);
            }
            server.log_dir = zstrdup(argv[1]);
        } else if (!strcasecmp(argv[0],"log_prefix") && argc == 2) {
            zfree(server.log_prefix);
            server.log_prefix = zstrdup(argv[1]);
        } else if (!strcasecmp(argv[0],"log_file_len") && argc == 2) {
            server.log_file_len = atoi(argv[1]);
        } else if (!strcasecmp(argv[0],"maxclients") && argc == 2) {
            server.maxclients = atoi(argv[1]);
        } else if (!strcasecmp(argv[0],"maxmemory") && argc == 2) {
            server.maxmemory = memtoll(argv[1],NULL);
        } else if (!strcasecmp(argv[0],"maxmemory-policy") && argc == 2) {
            if (!strcasecmp(argv[1],"volatile-lru")) {
                server.maxmemory_policy = REDIS_MAXMEMORY_VOLATILE_LRU;
            } else if (!strcasecmp(argv[1],"volatile-random")) {
                server.maxmemory_policy = REDIS_MAXMEMORY_VOLATILE_RANDOM;
            } else if (!strcasecmp(argv[1],"volatile-ttl")) {
                server.maxmemory_policy = REDIS_MAXMEMORY_VOLATILE_TTL;
            } else if (!strcasecmp(argv[1],"allkeys-lru")) {
                server.maxmemory_policy = REDIS_MAXMEMORY_ALLKEYS_LRU;
            } else if (!strcasecmp(argv[1],"allkeys-random")) {
                server.maxmemory_policy = REDIS_MAXMEMORY_ALLKEYS_RANDOM;
            } else if (!strcasecmp(argv[1],"noeviction")) {
                server.maxmemory_policy = REDIS_MAXMEMORY_NO_EVICTION;
            } else {
                err = "Invalid maxmemory policy";
                goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"maxmemory-samples") && argc == 2) {
            server.maxmemory_samples = atoi(argv[1]);
            if (server.maxmemory_samples <= 0) {
                err = "maxmemory-samples must be 1 or greater";
                goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"rdbcompression") && argc == 2) {
            if ((server.rdbcompression = yesnotoi(argv[1])) == -1) {
                err = "argument must be 'yes' or 'no'"; goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"daemonize") && argc == 2) {
            if ((server.daemonize = yesnotoi(argv[1])) == -1) {
                err = "argument must be 'yes' or 'no'"; goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"requirepass") && argc == 2) {
            server.requirepass = zstrdup(argv[1]);
        } else if (!strcasecmp(argv[0],"pidfile") && argc == 2) {
            if (server.pidfile)
            {
                zfree(server.pidfile);
            }
            server.pidfile = zstrdup(argv[1]);
        } else if (!strcasecmp(argv[0],"hash-max-zipmap-entries") && argc == 2) {
            server.hash_max_zipmap_entries = memtoll(argv[1], NULL);
        } else if (!strcasecmp(argv[0],"hash-max-zipmap-value") && argc == 2) {
            server.hash_max_zipmap_value = memtoll(argv[1], NULL);
        } else if (!strcasecmp(argv[0],"list-max-ziplist-entries") && argc == 2){
            server.list_max_ziplist_entries = memtoll(argv[1], NULL);
        } else if (!strcasecmp(argv[0],"list-max-ziplist-value") && argc == 2) {
            server.list_max_ziplist_value = memtoll(argv[1], NULL);
        } else if (!strcasecmp(argv[0],"set-max-intset-entries") && argc == 2) {
            server.set_max_intset_entries = memtoll(argv[1], NULL);
        } else if (!strcasecmp(argv[0],"zset-max-ziplist-entries") && argc == 2) {
            server.zset_max_ziplist_entries = memtoll(argv[1], NULL);
        } else if (!strcasecmp(argv[0],"zset-max-ziplist-value") && argc == 2) {
            server.zset_max_ziplist_value = memtoll(argv[1], NULL);
        } else if (!strcasecmp(argv[0],"slowlog-log-slower-than") &&
                   argc == 2)
        {
            server.slowlog_log_slower_than = strtoll(argv[1],NULL,10);
        } else if (!strcasecmp(argv[0],"slowlog-max-len") && argc == 2) {
            server.slowlog_max_len = strtoll(argv[1],NULL,10);
        } else if (!strcasecmp(argv[0],"app_path") && argc == 2) {
            zfree(server.app_path);
            server.app_path = zstrdup(argv[1]);
        } else if (!strcasecmp(argv[0],"dbe_root_path") && argc == 2) {
            if (server.dbe_root_path)
            {
                zfree(server.dbe_root_path);
            }
            server.dbe_root_path = zstrdup(argv[1]);
        } else if (!strcasecmp(argv[0],"binlog_root_path") && argc == 2) {
            if (server.bl_root_path)
            {
                zfree(server.bl_root_path);
            }
            server.bl_root_path = zstrdup(argv[1]);
        } else if (!strcasecmp(argv[0],"binlog_prefix") && argc == 2) {
            zfree(server.binlog_prefix);
            server.binlog_prefix = zstrdup(argv[1]);
        } else if (!strcasecmp(argv[0],"upd_cp_timeout") && argc == 2) {
            server.upd_cp_timeout = atoi(argv[1]);
        } else if (!strcasecmp(argv[0],"binlog_max_size") && argc == 2) {
            server.binlog_max_size = atoi(argv[1]);
        } else if (!strcasecmp(argv[0],"op_max_num") && argc == 2) {
            server.op_max_num = atoi(argv[1]);
        } else if (!strcasecmp(argv[0],"mngr_list") && argc == 2) {
            server.mngr_list = zstrdup(argv[1]);
            set_mngr_list(argv[1]);
        } else if (!strcasecmp(argv[0],"period") && argc == 2) {
            server.period = atoi(argv[1]);
        } else if (!strcasecmp(argv[0],"cache_filter") && argc >= 2) {
            const int perm = atoi(argv[1]);
            if (perm == -1)
            {
                set_cache_filter_list(0, 0, -1);
            }
            else if (perm == 0 || perm == 1)
            {
                if (argc > 2)
                {
                    set_cache_filter_list(argv[2], strlen(argv[2]), perm);
                }
                else
                {
                    set_cache_filter_list(0, 0, perm);
                }
            }
        } else if (!strcasecmp(argv[0],"bl_filter") && argc >= 2) {
            const int perm = atoi(argv[1]);
            if (perm == -1)
            {
                set_bl_filter_list(0, 0, -1);
            }
            else if (perm == 0 || perm == 1)
            {
                if (argc > 2)
                {
                    set_bl_filter_list(argv[2], strlen(argv[2]), perm);
                }
                else
                {
                    set_bl_filter_list(0, 0, perm);
                }
            }
        }
        for (j = 0; j < argc; j++)
            sdsfree(argv[j]);
        zfree(argv);
        sdsfree(line);
    }
    if (fp != stdin) fclose(fp);
    return;

loaderr:
    fprintf(stderr, "\n*** FATAL CONFIG FILE ERROR ***\n");
    fprintf(stderr, "Reading the configuration file, at line %d\n", linenum);
    fprintf(stderr, ">>> '%s'\n", line);
    fprintf(stderr, "%s\n", err);
    exit(1);
}

/*-----------------------------------------------------------------------------
 * CONFIG command for remote configuration
 *----------------------------------------------------------------------------*/

void configSetCommand(redisClient *c) 
{
    robj *o;
    long long ll;
    redisAssert(c->argv[2]->encoding == REDIS_ENCODING_RAW);
    redisAssert(c->argv[3]->encoding == REDIS_ENCODING_RAW);
    o = c->argv[3];

    if (!strcasecmp(c->argv[2]->ptr,"dbfilename")) {
        zfree(server.dbfilename);
        server.dbfilename = zstrdup(o->ptr);
    } else if (!strcasecmp(c->argv[2]->ptr,"requirepass")) {
        zfree(server.requirepass);
        server.requirepass = ((char*)o->ptr)[0] ? zstrdup(o->ptr) : NULL;
        redisLog(REDIS_PROMPT
                , "set requirepass: %s"
                , server.requirepass ? server.requirepass : "");
    } else if (!strcasecmp(c->argv[2]->ptr,"masterauth")) {
        zfree(server.masterauth);
        server.masterauth = zstrdup(o->ptr);
    } else if (!strcasecmp(c->argv[2]->ptr,"maxmemory")) {
        if (getLongLongFromObject(o,&ll) == REDIS_ERR ||
            ll < 0) goto badfmt;
        server.maxmemory = ll;
        if (server.maxmemory) freeMemoryIfNeeded();
    } else if (!strcasecmp(c->argv[2]->ptr,"maxmemory-policy")) {
        if (!strcasecmp(o->ptr,"volatile-lru")) {
            server.maxmemory_policy = REDIS_MAXMEMORY_VOLATILE_LRU;
        } else if (!strcasecmp(o->ptr,"volatile-random")) {
            server.maxmemory_policy = REDIS_MAXMEMORY_VOLATILE_RANDOM;
        } else if (!strcasecmp(o->ptr,"volatile-ttl")) {
            server.maxmemory_policy = REDIS_MAXMEMORY_VOLATILE_TTL;
        } else if (!strcasecmp(o->ptr,"allkeys-lru")) {
            server.maxmemory_policy = REDIS_MAXMEMORY_ALLKEYS_LRU;
        } else if (!strcasecmp(o->ptr,"allkeys-random")) {
            server.maxmemory_policy = REDIS_MAXMEMORY_ALLKEYS_RANDOM;
        } else if (!strcasecmp(o->ptr,"noeviction")) {
            server.maxmemory_policy = REDIS_MAXMEMORY_NO_EVICTION;
        } else {
            goto badfmt;
        }
    } else if (!strcasecmp(c->argv[2]->ptr,"maxmemory-samples")) {
        if (getLongLongFromObject(o,&ll) == REDIS_ERR ||
            ll <= 0) goto badfmt;
        server.maxmemory_samples = ll;
    } else if (!strcasecmp(c->argv[2]->ptr,"timeout")) {
        if (getLongLongFromObject(o,&ll) == REDIS_ERR ||
            ll < 0 || ll > LONG_MAX) goto badfmt;
        server.maxidletime = ll;
    } else if (!strcasecmp(c->argv[2]->ptr,"appendfsync")) {
        if (!strcasecmp(o->ptr,"no")) {
            server.appendfsync = APPENDFSYNC_NO;
        } else if (!strcasecmp(o->ptr,"everysec")) {
            server.appendfsync = APPENDFSYNC_EVERYSEC;
        } else if (!strcasecmp(o->ptr,"always")) {
            server.appendfsync = APPENDFSYNC_ALWAYS;
        } else {
            goto badfmt;
        }
    } else if (!strcasecmp(c->argv[2]->ptr,"no-appendfsync-on-rewrite")) {
        int yn = yesnotoi(o->ptr);

        if (yn == -1) goto badfmt;
        server.no_appendfsync_on_rewrite = yn;
    } else if (!strcasecmp(c->argv[2]->ptr,"appendonly")) {
#if 0
        // disable in FooYun
        int old = server.appendonly;
        int new = yesnotoi(o->ptr);

        if (new == -1) goto badfmt;
        if (old != new) {
            if (new == 0) {
                stopAppendOnly();
            } else {
                if (startAppendOnly() == REDIS_ERR) {
                    addReplyError(c,
                        "Unable to turn on AOF. Check server logs.");
                    return;
                }
            }
        }
#endif
    } else if (!strcasecmp(c->argv[2]->ptr,"auto-aof-rewrite-percentage")) {
        if (getLongLongFromObject(o,&ll) == REDIS_ERR || ll < 0) goto badfmt;
        server.auto_aofrewrite_perc = ll;
    } else if (!strcasecmp(c->argv[2]->ptr,"auto-aof-rewrite-min-size")) {
        if (getLongLongFromObject(o,&ll) == REDIS_ERR || ll < 0) goto badfmt;
        server.auto_aofrewrite_min_size = ll;
    } else if (!strcasecmp(c->argv[2]->ptr,"save")) {
        int vlen, j;
        sds *v = sdssplitlen(o->ptr,sdslen(o->ptr)," ",1,&vlen);

        /* Perform sanity check before setting the new config:
         * - Even number of args
         * - Seconds >= 1, changes >= 0 */
        if (vlen & 1) {
            sdsfreesplitres(v,vlen);
            goto badfmt;
        }
        for (j = 0; j < vlen; j++) {
            char *eptr;
            long val;

            val = strtoll(v[j], &eptr, 10);
            if (eptr[0] != '\0' ||
                ((j & 1) == 0 && val < 1) ||
                ((j & 1) == 1 && val < 0)) {
                sdsfreesplitres(v,vlen);
                goto badfmt;
            }
        }
        /* Finally set the new config */
        resetServerSaveParams();
        for (j = 0; j < vlen; j += 2) {
            time_t seconds;
            int changes;

            seconds = strtoll(v[j],NULL,10);
            changes = strtoll(v[j+1],NULL,10);
            appendServerSaveParams(seconds, changes);
        }
        sdsfreesplitres(v,vlen);
    } else if (!strcasecmp(c->argv[2]->ptr,"slave-serve-stale-data")) {
        int yn = yesnotoi(o->ptr);

        if (yn == -1) goto badfmt;
        server.repl_serve_stale_data = yn;
    } else if (!strcasecmp(c->argv[2]->ptr,"dir")) {
        if (chdir((char*)o->ptr) == -1) {
            addReplyErrorFormat(c,"Changing directory: %s", strerror(errno));
            return;
        }
    } else if (!strcasecmp(c->argv[2]->ptr,"hash-max-zipmap-entries")) {
        if (getLongLongFromObject(o,&ll) == REDIS_ERR || ll < 0) goto badfmt;
        server.hash_max_zipmap_entries = ll;
    } else if (!strcasecmp(c->argv[2]->ptr,"hash-max-zipmap-value")) {
        if (getLongLongFromObject(o,&ll) == REDIS_ERR || ll < 0) goto badfmt;
        server.hash_max_zipmap_value = ll;
    } else if (!strcasecmp(c->argv[2]->ptr,"list-max-ziplist-entries")) {
        if (getLongLongFromObject(o,&ll) == REDIS_ERR || ll < 0) goto badfmt;
        server.list_max_ziplist_entries = ll;
    } else if (!strcasecmp(c->argv[2]->ptr,"list-max-ziplist-value")) {
        if (getLongLongFromObject(o,&ll) == REDIS_ERR || ll < 0) goto badfmt;
        server.list_max_ziplist_value = ll;
    } else if (!strcasecmp(c->argv[2]->ptr,"set-max-intset-entries")) {
        if (getLongLongFromObject(o,&ll) == REDIS_ERR || ll < 0) goto badfmt;
        server.set_max_intset_entries = ll;
    } else if (!strcasecmp(c->argv[2]->ptr,"zset-max-ziplist-entries")) {
        if (getLongLongFromObject(o,&ll) == REDIS_ERR || ll < 0) goto badfmt;
        server.zset_max_ziplist_entries = ll;
    } else if (!strcasecmp(c->argv[2]->ptr,"zset-max-ziplist-value")) {
        if (getLongLongFromObject(o,&ll) == REDIS_ERR || ll < 0) goto badfmt;
        server.zset_max_ziplist_value = ll;
    } else if (!strcasecmp(c->argv[2]->ptr,"slowlog-log-slower-than")) {
        if (getLongLongFromObject(o,&ll) == REDIS_ERR) goto badfmt;
        server.slowlog_log_slower_than = ll;
    } else if (!strcasecmp(c->argv[2]->ptr,"slowlog-max-len")) {
        if (getLongLongFromObject(o,&ll) == REDIS_ERR || ll < 0) goto badfmt;
        server.slowlog_max_len = (unsigned)ll;
    } else if (!strcasecmp(c->argv[2]->ptr,"slow_log")) {
        if (getLongLongFromObject(o,&ll) == REDIS_ERR) goto badfmt;
        server.slow_log = ll == 0 ? 0 : 1;
    } else if (!strcasecmp(c->argv[2]->ptr,"min_mem")) {
        if (getLongLongFromObject(o,&ll) == REDIS_ERR) goto badfmt;
        if (ll > 11 * 1024 * 1024) server.db_min_size = ll;
    } else if (!strcasecmp(c->argv[2]->ptr,"max_mem")) {
        if (getLongLongFromObject(o,&ll) == REDIS_ERR) goto badfmt;
        if (ll > server.db_min_size) server.db_max_size = ll;
    } else if (!strcasecmp(c->argv[2]->ptr,"read_dbe")) {
        if (getLongLongFromObject(o,&ll) == REDIS_ERR) goto badfmt;
        server.read_dbe = ll == 0 ? 0 : 1;
    } else if (!strcasecmp(c->argv[2]->ptr,"wr_bl")) {
        if (getLongLongFromObject(o,&ll) == REDIS_ERR) goto badfmt;
        server.wr_bl = ll == 0 ? 0 : 1;
    } else if (!strcasecmp(c->argv[2]->ptr,"compress")) {
        if (getLongLongFromObject(o,&ll) == REDIS_ERR) goto badfmt;
        server.rdbcompression = ll == 0 ? 0 : 1;
    } else if (!strcasecmp(c->argv[2]->ptr,"log_get_miss")) {
        if (getLongLongFromObject(o,&ll) == REDIS_ERR) goto badfmt;
        server.log_get_miss = ll == 0 ? 0 : 1;
    } else if (!strcasecmp(c->argv[2]->ptr,"dbe_get_que_size")) {
        if (getLongLongFromObject(o,&ll) == REDIS_ERR) goto badfmt;
        if (ll > 0) server.dbe_get_que_size = ll;
    } else if (!strcasecmp(c->argv[2]->ptr,"wr_bl_que_size")) {
        if (getLongLongFromObject(o,&ll) == REDIS_ERR) goto badfmt;
        if (ll > 0) server.wr_bl_que_size = ll;
    } else if (!strcasecmp(c->argv[2]->ptr,"loglevel")) {
        if (!strcasecmp(o->ptr,"warning")) {
            server.verbosity = REDIS_WARNING;
        } else if (!strcasecmp(o->ptr,"notice")) {
            server.verbosity = REDIS_NOTICE;
        } else if (!strcasecmp(o->ptr,"verbose")) {
            server.verbosity = REDIS_VERBOSE;
        } else if (!strcasecmp(o->ptr,"debug")) {
            server.verbosity = REDIS_DEBUG;
        } else {
            goto badfmt;
        }
        pf_log_set_level(server.verbosity);
    } else {
        addReplyErrorFormat(c,"Unsupported CONFIG parameter: %s",
            (char*)c->argv[2]->ptr);
        return;
    }
    addReply(c,shared.ok);
    return;

badfmt: /* Bad format errors */
    addReplyErrorFormat(c,"Invalid argument '%s' for CONFIG SET '%s'",
            (char*)o->ptr,
            (char*)c->argv[2]->ptr);
}

void configGetCommand(redisClient *c) {
    robj *o = c->argv[2];
    void *replylen = addDeferredMultiBulkLength(c);
    char *pattern = o->ptr;
    char buf[128];
    int matches = 0;
    redisAssert(o->encoding == REDIS_ENCODING_RAW);

    if (stringmatch(pattern,"dir",0)) {
        char buf[1024];

        if (getcwd(buf,sizeof(buf)) == NULL)
            buf[0] = '\0';

        addReplyBulkCString(c,"dir");
        addReplyBulkCString(c,buf);
        matches++;
    }
    if (stringmatch(pattern,"dbfilename",0)) {
        addReplyBulkCString(c,"dbfilename");
        addReplyBulkCString(c,server.dbfilename);
        matches++;
    }
    if (stringmatch(pattern,"requirepass",0)) {
        addReplyBulkCString(c,"requirepass");
        addReplyBulkCString(c,server.requirepass);
        matches++;
    }
    if (stringmatch(pattern,"masterauth",0)) {
        addReplyBulkCString(c,"masterauth");
        addReplyBulkCString(c,server.masterauth);
        matches++;
    }
    if (stringmatch(pattern,"maxmemory",0)) {
        ll2string(buf,sizeof(buf),server.maxmemory);
        addReplyBulkCString(c,"maxmemory");
        addReplyBulkCString(c,buf);
        matches++;
    }
    if (stringmatch(pattern,"maxmemory-policy",0)) {
        char *s;

        switch(server.maxmemory_policy) {
        case REDIS_MAXMEMORY_VOLATILE_LRU: s = "volatile-lru"; break;
        case REDIS_MAXMEMORY_VOLATILE_TTL: s = "volatile-ttl"; break;
        case REDIS_MAXMEMORY_VOLATILE_RANDOM: s = "volatile-random"; break;
        case REDIS_MAXMEMORY_ALLKEYS_LRU: s = "allkeys-lru"; break;
        case REDIS_MAXMEMORY_ALLKEYS_RANDOM: s = "allkeys-random"; break;
        case REDIS_MAXMEMORY_NO_EVICTION: s = "noeviction"; break;
        default: s = "unknown"; break; /* too harmless to panic */
        }
        addReplyBulkCString(c,"maxmemory-policy");
        addReplyBulkCString(c,s);
        matches++;
    }
    if (stringmatch(pattern,"maxmemory-samples",0)) {
        ll2string(buf,sizeof(buf),server.maxmemory_samples);
        addReplyBulkCString(c,"maxmemory-samples");
        addReplyBulkCString(c,buf);
        matches++;
    }
    if (stringmatch(pattern,"timeout",0)) {
        ll2string(buf,sizeof(buf),server.maxidletime);
        addReplyBulkCString(c,"timeout");
        addReplyBulkCString(c,buf);
        matches++;
    }
    if (stringmatch(pattern,"appendonly",0)) {
        addReplyBulkCString(c,"appendonly");
        addReplyBulkCString(c,server.appendonly ? "yes" : "no");
        matches++;
    }
    if (stringmatch(pattern,"no-appendfsync-on-rewrite",0)) {
        addReplyBulkCString(c,"no-appendfsync-on-rewrite");
        addReplyBulkCString(c,server.no_appendfsync_on_rewrite ? "yes" : "no");
        matches++;
    }
    if (stringmatch(pattern,"appendfsync",0)) {
        char *policy;

        switch(server.appendfsync) {
        case APPENDFSYNC_NO: policy = "no"; break;
        case APPENDFSYNC_EVERYSEC: policy = "everysec"; break;
        case APPENDFSYNC_ALWAYS: policy = "always"; break;
        default: policy = "unknown"; break; /* too harmless to panic */
        }
        addReplyBulkCString(c,"appendfsync");
        addReplyBulkCString(c,policy);
        matches++;
    }
    if (stringmatch(pattern,"save",0)) {
        sds buf = sdsempty();
        int j;

        for (j = 0; j < server.saveparamslen; j++) {
            buf = sdscatprintf(buf,"%ld %d",
                    server.saveparams[j].seconds,
                    server.saveparams[j].changes);
            if (j != server.saveparamslen-1)
                buf = sdscatlen(buf," ",1);
        }
        addReplyBulkCString(c,"save");
        addReplyBulkCString(c,buf);
        sdsfree(buf);
        matches++;
    }
    if (stringmatch(pattern,"auto-aof-rewrite-percentage",0)) {
        addReplyBulkCString(c,"auto-aof-rewrite-percentage");
        addReplyBulkLongLong(c,server.auto_aofrewrite_perc);
        matches++;
    }
    if (stringmatch(pattern,"auto-aof-rewrite-min-size",0)) {
        addReplyBulkCString(c,"auto-aof-rewrite-min-size");
        addReplyBulkLongLong(c,server.auto_aofrewrite_min_size);
        matches++;
    }
    if (stringmatch(pattern,"slave-serve-stale-data",0)) {
        addReplyBulkCString(c,"slave-serve-stale-data");
        addReplyBulkCString(c,server.repl_serve_stale_data ? "yes" : "no");
        matches++;
    }
    if (stringmatch(pattern,"hash-max-zipmap-entries",0)) {
        addReplyBulkCString(c,"hash-max-zipmap-entries");
        addReplyBulkLongLong(c,server.hash_max_zipmap_entries);
        matches++;
    }
    if (stringmatch(pattern,"hash-max-zipmap-value",0)) {
        addReplyBulkCString(c,"hash-max-zipmap-value");
        addReplyBulkLongLong(c,server.hash_max_zipmap_value);
        matches++;
    }
    if (stringmatch(pattern,"list-max-ziplist-entries",0)) {
        addReplyBulkCString(c,"list-max-ziplist-entries");
        addReplyBulkLongLong(c,server.list_max_ziplist_entries);
        matches++;
    }
    if (stringmatch(pattern,"list-max-ziplist-value",0)) {
        addReplyBulkCString(c,"list-max-ziplist-value");
        addReplyBulkLongLong(c,server.list_max_ziplist_value);
        matches++;
    }
    if (stringmatch(pattern,"set-max-intset-entries",0)) {
        addReplyBulkCString(c,"set-max-intset-entries");
        addReplyBulkLongLong(c,server.set_max_intset_entries);
        matches++;
    }
    if (stringmatch(pattern,"zset-max-ziplist-entries",0)) {
        addReplyBulkCString(c,"zset-max-ziplist-entries");
        addReplyBulkLongLong(c,server.zset_max_ziplist_entries);
        matches++;
    }
    if (stringmatch(pattern,"zset-max-ziplist-value",0)) {
        addReplyBulkCString(c,"zset-max-ziplist-value");
        addReplyBulkLongLong(c,server.zset_max_ziplist_value);
        matches++;
    }
    if (stringmatch(pattern,"slowlog-log-slower-than",0)) {
        addReplyBulkCString(c,"slowlog-log-slower-than");
        addReplyBulkLongLong(c,server.slowlog_log_slower_than);
        matches++;
    }
    if (stringmatch(pattern,"slowlog-max-len",0)) {
        addReplyBulkCString(c,"slowlog-max-len");
        addReplyBulkLongLong(c,server.slowlog_max_len);
        matches++;
    }
    if (stringmatch(pattern,"loglevel",0)) {
        char *s;

        switch(server.verbosity) {
        case REDIS_WARNING: s = "warning"; break;
        case REDIS_VERBOSE: s = "verbose"; break;
        case REDIS_NOTICE: s = "notice"; break;
        case REDIS_DEBUG: s = "debug"; break;
        default: s = "unknown"; break; /* too harmless to panic */
        }
        addReplyBulkCString(c,"loglevel");
        addReplyBulkCString(c,s);
        matches++;
    }
    setDeferredMultiBulkLength(c,replylen,matches*2);
}

void configCommand(redisClient *c) {
    if (!strcasecmp(c->argv[1]->ptr,"set")) {
        if (c->argc != 4) goto badarity;
        configSetCommand(c);
    } else if (!strcasecmp(c->argv[1]->ptr,"get")) {
        if (c->argc == 3)
        {
            configGetCommand(c);
        }
        else
        {
            goto badarity;
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"resetstat")) {
        if (c->argc != 2) goto badarity;
        server.stat_keyspace_hits = 0;
        server.stat_keyspace_misses = 0;
        server.stat_numcommands = 0;
        server.stat_numconnections = 0;
        server.stat_expiredkeys = 0;
        addReply(c,shared.ok);
    } else {
        addReplyError(c,
            "CONFIG subcommand must be one of GET, SET, RESETSTAT");
    }
    return;

badarity:
    addReplyErrorFormat(c,"Wrong number of arguments for CONFIG %s",
        (char*) c->argv[1]->ptr);
}
