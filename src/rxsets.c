/*
* rxsets - extended Redis Set commands module.
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
#include "../redismodule.h"
#include "../rmutil/util.h"
#include "../rmutil/test_util.h"

#define RM_MODULE_NAME "rxsets"

/*
* MSISMEMBER key1 [key2 ...] member
* Checks for membership in multiple sets.
* Reply: Integer, the count of sets to which the member belongs.
*/
int MSIsMemberCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 3) {
    if (RedisModule_IsKeysPositionRequest(ctx))
      /* TODO: handle this once the getkey-api allows signalling errors */
      return REDISMODULE_OK;
    else
      return RedisModule_WrongArity(ctx);
  }

  if (RedisModule_IsKeysPositionRequest(ctx)) {
    size_t i;
    for (i = 1; i < argc - 1; i++) RedisModule_KeyAtPos(ctx, i);
    return REDISMODULE_OK;
  }

  RedisModule_AutoMemory(ctx);

  int iele = argc - 1;
  size_t count = 0;
  int i;
  for (i = 1; i < iele; i++) {
    RedisModuleKey *key =
        RedisModule_OpenKey(ctx, argv[i], REDISMODULE_READ | REDISMODULE_WRITE);

    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) continue;

    if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_SET) {
      RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
      return REDISMODULE_ERR;
    }

    RedisModuleCallReply *rep =
        RedisModule_Call(ctx, "SISMEMBER", "ss", argv[i], argv[iele]);
    RMUTIL_ASSERT_NOERROR(rep)

    count += RedisModule_CallReplyInteger(rep);
  }

  RedisModule_ReplyWithLongLong(ctx, count);
  return REDISMODULE_OK;
}

int testMSIsMember(RedisModuleCtx *ctx) {
  RedisModuleCallReply *r;

  r = RedisModule_Call(ctx, "msismember", "ccc", "s1", "s2", "ele");
  RMUtil_Assert(RedisModule_CallReplyInteger(r) == 0);
  r = RedisModule_Call(ctx, "SADD", "ccc", "s1", "ele", "foo");
  r = RedisModule_Call(ctx, "msismember", "ccc", "s1", "s2", "ele");
  RMUtil_Assert(RedisModule_CallReplyInteger(r) == 1);
  r = RedisModule_Call(ctx, "SADD", "cc", "s2", "bar");
  r = RedisModule_Call(ctx, "msismember", "ccc", "s1", "s2", "ele");
  RMUtil_Assert(RedisModule_CallReplyInteger(r) == 1);
  r = RedisModule_Call(ctx, "SADD", "cc", "s2", "ele");
  r = RedisModule_Call(ctx, "msismember", "ccc", "s1", "s2", "ele");
  RMUtil_Assert(RedisModule_CallReplyInteger(r) == 2);

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

  RMUtil_Test(testMSIsMember);

  RedisModule_ReplyWithSimpleString(ctx, "PASS");
  return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx) {
  if (RedisModule_Init(ctx, RM_MODULE_NAME, 1, REDISMODULE_APIVER_1) ==
      REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "msismember", MSIsMemberCommand,
                                "readonly fast getkeys-api", 0, 0,
                                0) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "rxsets.test", TestModule, "write", 0, 0,
                                0) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  return REDISMODULE_OK;
}