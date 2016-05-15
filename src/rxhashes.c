/*
* rxhashes - extended Redis Hash commands module.
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
#include "../rmutil/strings.h"
#include "../rmutil/test_util.h"

#define RM_MODULE_NAME "rxhashes"

/*
* HGETSET key field value
* Sets the 'field' in Hash 'key' to 'value' and returns the previous value, if
* any.
* Reply: String, the previous value or NULL if 'field' didn't exist.
*/
int HGetSetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  RedisModule_AutoMemory(ctx);

  // open the key and make sure it is indeed a Hash and not empty
  RedisModuleKey *key =
      RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);

  if ((RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY) &&
      (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_HASH)) {
    RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    return REDISMODULE_ERR;
  }

  // get the current value of the hash element
  RedisModuleString *val;
  RedisModule_HashGet(key, REDISMODULE_HASH_NONE, argv[2], &val, NULL);

  // set the element to the new value
  RedisModule_HashSet(key, REDISMODULE_HASH_NONE, argv[2], argv[3], NULL);

  if (!val)
    RedisModule_ReplyWithNull(ctx);
  else
    RedisModule_ReplyWithString(ctx, val);
  return REDISMODULE_OK;
}

int testHGetSet(RedisModuleCtx *ctx) {
  RedisModuleCallReply *r;

  r = RedisModule_Call(ctx, "hgetset", "ccc", "foo", "bar", "baz");
  RMUtil_Assert(RedisModule_CallReplyType(r) == REDISMODULE_REPLY_NULL);
  r = RedisModule_Call(ctx, "hgetset", "ccc", "foo", "bar", "qaz");
  RMUtil_AssertReplyEquals(r, "baz");
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

  RMUtil_Test(testHGetSet);

  RedisModule_ReplyWithSimpleString(ctx, "PASS");
  return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx) {
  if (RedisModule_Init(ctx, RM_MODULE_NAME, 1, REDISMODULE_APIVER_1) ==
      REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "hgetset", HGetSetCommand,
                                "write fast deny-oom", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "rxhashes.test", TestModule, "write", 0,
                                0, 0) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  return REDISMODULE_OK;
}