/*
* rxgeo - extended Redis Geo Set commands module.
* Copyright (C) 2016 Redis Labs
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License as
* published by the Free Software Foundation, either version 3 of the
* License, or (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU Affero General Public License for more details.
*
* You should have received a copy of the GNU Affero General Public License
* along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
#include <string.h>
#include "../redismodule.h"
#include "../rmutil/util.h"
#include "../rmutil/strings.h"
#include "../rmutil/test_util.h"

#define RM_MODULE_NAME "rxgeo"

/*
* GEOCLUSTER geoset radius unit min-points [namespace]
* The default cluster `namespace` is 'DBRS'.
* Density based spatial clustering with random sampling.
* http://www.ucalgary.ca/wangx/files/wangx/comparison.pdf
* http://www.ucalgary.ca/wangx/files/wangx/dbrs.pdf
* Note: not really random because the geoset is ordered.
* Reply: Integer, the number of clusters created
*/
int GeoClusterCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if ((argc < 5) || (argc > 6)) {
    return RedisModule_WrongArity(ctx);
  }
  RedisModule_AutoMemory(ctx);

  // open the key and make sure it is indeed a Zset
  RedisModuleKey *key =
      RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);

  /* Check that the key exists. */
  if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
    RedisModule_ReplyWithNull(ctx);
    return REDISMODULE_OK;
  }

  /* Check that the key is a zset. */
  if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_ZSET) {
    RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    return REDISMODULE_ERR;
  }
  size_t klen;
  const char *kstr = RedisModule_StringPtrLen(argv[1], &klen);

  /* Epsilon. */
  long long eps;
  if ((RedisModule_StringToLongLong(argv[2], &eps) != REDISMODULE_OK) ||
      (eps < 0)) {
    RedisModule_ReplyWithError(ctx, "ERR radius has to be a positive integer");
    return REDISMODULE_ERR;
  }

  /* Epsilon unit. */
  size_t ulen;
  const char *unit = RedisModule_StringPtrLen(argv[3], &ulen);
  if (strcasecmp("m", unit) && strcasecmp("km", unit) &&
      strcasecmp("ft", unit) && strcasecmp("mi", unit)) {
    RedisModule_ReplyWithError(
        ctx, "ERR unknown unit - must be 'm', 'km', 'ft' or 'mi'");
    return REDISMODULE_ERR;
  }

  /* Namespace. */
  size_t nslen;
  const char *nsstr;
  if (argc == 6) {
    nsstr = RedisModule_StringPtrLen(argv[5], &nslen);
  } else {
    nsstr = "DBRS";
    nslen = strlen(nsstr);
  }

  /* Minimum points in a cluster. */
  long long minpts;
  if ((RedisModule_StringToLongLong(argv[4], &minpts) != REDISMODULE_OK) ||
      (minpts < 0)) {
    RedisModule_ReplyWithError(ctx, "ERR minpts has to be a positive integer");
    return REDISMODULE_ERR;
  }

  /* Delete the namespace's clusters & list, if any. */
  RedisModuleString *krmsCL = RMUtil_CreateFormattedString(
      ctx, "%.*s:{%.*s}:CL", nslen, nsstr, klen, kstr);
  RedisModuleKey *keyCL =
      RedisModule_OpenKey(ctx, krmsCL, REDISMODULE_READ | REDISMODULE_WRITE);
  if (RedisModule_KeyType(keyCL) != REDISMODULE_KEYTYPE_EMPTY) {
    while (RedisModule_ValueLength(keyCL)) {
      RedisModuleString *ele =
          RedisModule_ListPop(keyCL, REDISMODULE_LIST_HEAD);
      RedisModuleKey *keyC =
          RedisModule_OpenKey(ctx, ele, REDISMODULE_READ | REDISMODULE_WRITE);
      if (RedisModule_KeyType(keyC) != REDISMODULE_KEYTYPE_EMPTY)
        RedisModule_DeleteKey(keyC);
      RedisModule_CloseKey(keyC);
      RedisModule_FreeString(ctx, ele);
    }
    RedisModule_DeleteKey(keyCL);
  }
  /* Keys in the engine. */
  size_t j = 1;

  /* Start DBRS. */
  RedisModule_ZsetFirstInScoreRange(key, REDISMODULE_NEGATIVE_INFINITE,
                                    REDISMODULE_POSITIVE_INFINITE, 0, 0);

  while (!RedisModule_ZsetRangeEndReached(key)) {
    /* Pick q, in our case the current element in the geo set */
    RedisModuleString *q = RedisModule_ZsetRangeCurrentElement(key, NULL);

    /* Get & store qseeds with a call to GEORADIUSBYMEMBER. */
    RedisModuleString *qseeds = RMUtil_CreateFormattedString(
        ctx, "%.*s:{%.*s}:qseeds", nslen, nsstr, klen, kstr);
    RedisModuleCallReply *trep =
        RedisModule_Call(ctx, "GEORADIUSBYMEMBER", "sssscs", argv[1], q,
                         argv[2], argv[3], "STORE", qseeds);
    long long nqseeds = RedisModule_CallReplyInteger(trep);
    RedisModule_FreeCallReply(trep);
    RedisModuleKey *keyQseeds =
        RedisModule_OpenKey(ctx, qseeds, REDISMODULE_READ | REDISMODULE_WRITE);

    /* j is always growing when used. */
    RedisModuleString *Cj = RMUtil_CreateFormattedString(
        ctx, "%.*s:{%.*s}:%zu", nslen, nsstr, klen, kstr, j);

    /* Non-puritan version. */
    if (nqseeds >= minpts) {
      unsigned isFirstMerge = 1;
      RedisModuleString *newCi;
      size_t CLlen = RedisModule_ValueLength(keyCL);
      size_t i;
      for (i = 0; i < CLlen; i++) {
        RedisModuleString *Ci =
            RedisModule_ListPop(keyCL, REDISMODULE_LIST_HEAD);
        RedisModuleKey *keyCi =
            RedisModule_OpenKey(ctx, Ci, REDISMODULE_READ | REDISMODULE_WRITE);

        /* No need for a full intersect, hence do it manually and break fast if
         * possible. Remember that low cardinality matters (and that the dice
         * are loaded). */
        int hasIntersect = 0;
        RedisModuleKey *lc, *hc;
        if (nqseeds < RedisModule_ValueLength(keyCi)) {
          lc = keyQseeds;
          hc = keyCi;
        } else {
          lc = keyCi;
          hc = keyQseeds;
        }

        RedisModule_ZsetFirstInScoreRange(lc, REDISMODULE_NEGATIVE_INFINITE,
                                          REDISMODULE_POSITIVE_INFINITE, 0, 0);
        while (!RedisModule_ZsetRangeEndReached(lc)) {
          /* TODO: replace w/ RedisModule_ZsetRank when avail? */
          RedisModuleString *ele =
              RedisModule_ZsetRangeCurrentElement(lc, NULL);
          if (RedisModule_ZsetScore(hc, ele, NULL) == REDISMODULE_OK) {
            hasIntersect = 1;
            RedisModule_FreeString(ctx, ele);
            break;
          }
          RedisModule_FreeString(ctx, ele);
          RedisModule_ZsetRangeNext(lc);
        }
        RedisModule_ZsetRangeStop(lc);

        if (hasIntersect) {
          if (isFirstMerge) {
            trep = RedisModule_Call(ctx, "ZUNIONSTORE", "scss", Ci, "2", Ci,
                                    qseeds);
            RedisModule_FreeCallReply(trep);
            newCi = Ci;
            isFirstMerge = 0;
            /* Do the opposite of deleteCluster(Ci). */
            RedisModule_ListPush(keyCL, REDISMODULE_LIST_TAIL, Ci);
          } else {
            trep = RedisModule_Call(ctx, "ZUNIONSTORE", "scss", newCi, "2",
                                    newCi, Ci);
            RedisModule_FreeCallReply(trep);
            RedisModule_DeleteKey(keyCi);
          }
        } else {
          RedisModule_ListPush(keyCL, REDISMODULE_LIST_TAIL, Ci);
        }
        RedisModule_FreeString(ctx, Ci);
        RedisModule_CloseKey(keyCi);
      }
      if (isFirstMerge) {
        RedisModule_ListPush(keyCL, REDISMODULE_LIST_TAIL, Cj);
        /* TODO: Replace w/ RedisModule_KeyRename when avail. */
        trep = RedisModule_Call(ctx, "RENAME", "ss", qseeds, Cj);
        RedisModule_FreeCallReply(trep);
        j++;
      } else {
        RedisModule_DeleteKey(keyQseeds);
      }
    }
    RedisModule_FreeString(ctx, q);
    RedisModule_FreeString(ctx, qseeds);
    RedisModule_FreeString(ctx, Cj);
    RedisModule_DeleteKey(keyQseeds);
    RedisModule_CloseKey(keyQseeds);
    RedisModule_ZsetRangeNext(key);
  }
  RedisModule_ZsetRangeStop(key);
  RedisModule_CloseKey(key);

  RedisModule_CloseKey(keyCL);
  RedisModule_ReplyWithLongLong(ctx, RedisModule_ValueLength(keyCL));
  return REDISMODULE_OK;
}

int testGeoCluster(RedisModuleCtx *ctx) {
  RedisModuleCallReply *r;

  r = RedisModule_Call(ctx, "GEOADD", "cccc", "geoset", "0", "0", "1-1");
  r = RedisModule_Call(ctx, "GEOADD", "cccc", "geoset", "0.01", "0", "1-2");
  r = RedisModule_Call(ctx, "GEOADD", "cccc", "geoset", "0.01", "0.01", "1-3");
  r = RedisModule_Call(ctx, "GEOADD", "cccc", "geoset", "0", "0.01", "1-4");
  r = RedisModule_Call(ctx, "GEOADD", "cccc", "geoset", "10", "0", "2-1");
  r = RedisModule_Call(ctx, "GEOADD", "cccc", "geoset", "10.01", "0", "2-2");
  r = RedisModule_Call(ctx, "GEOADD", "cccc", "geoset", "10.01", "0.01", "2-3");
  r = RedisModule_Call(ctx, "GEOADD", "cccc", "geoset", "0", "10", "3-1");
  r = RedisModule_Call(ctx, "GEOADD", "cccc", "geoset", "50.01", "50", "4-1");
  r = RedisModule_Call(ctx, "GEOADD", "cccc", "geoset", "50.01", "50.01", "4-2");
  r = RedisModule_Call(ctx, "geocluster", "ccccc", "geoset", "100", "km", "3", "test");

  r = RedisModule_Call(ctx, "LLEN", "c", "test:{geoset}:CL");
  RMUtil_Assert(RedisModule_CallReplyInteger(r) == 2);
  r = RedisModule_Call(ctx, "ZCARD", "c", "test:{geoset}:1");
  RMUtil_Assert(RedisModule_CallReplyInteger(r) == 4);
  r = RedisModule_Call(ctx, "ZCARD", "c", "test:{geoset}:2");
  RMUtil_Assert(RedisModule_CallReplyInteger(r) == 3);
  r = RedisModule_Call(ctx, "ZSCORE", "cc", "test:{geoset}:1", "1-1");
  RMUtil_Assert(RedisModule_CallReplyType(r) != REDISMODULE_REPLY_NULL);
  r = RedisModule_Call(ctx, "ZSCORE", "cc", "test:{geoset}:1", "1-2");
  RMUtil_Assert(RedisModule_CallReplyType(r) != REDISMODULE_REPLY_NULL);
  r = RedisModule_Call(ctx, "ZSCORE", "cc", "test:{geoset}:1", "1-3");
  RMUtil_Assert(RedisModule_CallReplyType(r) != REDISMODULE_REPLY_NULL);
  r = RedisModule_Call(ctx, "ZSCORE", "cc", "test:{geoset}:1", "1-4");
  RMUtil_Assert(RedisModule_CallReplyType(r) != REDISMODULE_REPLY_NULL);
  r = RedisModule_Call(ctx, "ZSCORE", "cc", "test:{geoset}:2", "2-1");
  RMUtil_Assert(RedisModule_CallReplyType(r) != REDISMODULE_REPLY_NULL);
  r = RedisModule_Call(ctx, "ZSCORE", "cc", "test:{geoset}:2", "2-2");
  RMUtil_Assert(RedisModule_CallReplyType(r) != REDISMODULE_REPLY_NULL);
  r = RedisModule_Call(ctx, "ZSCORE", "cc", "test:{geoset}:2", "2-3");
  RMUtil_Assert(RedisModule_CallReplyType(r) != REDISMODULE_REPLY_NULL);

  r = RedisModule_Call(ctx, "FLUSHALL", "");

  return 0;
}

int TestModule(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_AutoMemory(ctx);

  /* TODO: calling flushall but checking only for db 0. */
  RedisModuleCallReply *r = RedisModule_Call(ctx, "DBSIZE", "");
  if (RedisModule_CallReplyInteger(r) != 0) {
    RedisModule_ReplyWithError(ctx,
                               "ERR test must be run on an empty instance");
    return REDISMODULE_ERR;
  }

  RMUtil_Test(testGeoCluster);

  RedisModule_ReplyWithSimpleString(ctx, "PASS");
  return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx) {
  if (RedisModule_Init(ctx, RM_MODULE_NAME, 1, REDISMODULE_APIVER_1) ==
      REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "geocluster", GeoClusterCommand,
                                "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "rxgeo.test", TestModule, "write", 0, 0,
                                0) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  return REDISMODULE_OK;
}