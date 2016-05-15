/*
* rxlists - extended Redis List commands module.
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
#include <stdlib.h>
#include <string.h>

#include "../redismodule.h"
#include "../rmutil/util.h"
#include "../rmutil/strings.h"
#include "../rmutil/test_util.h"

#define RM_MODULE_NAME "rxlists"

/* LSPLICE srclist dstlist count
 * Moves 'count' elements from the tail of 'srclist' to the head of
 * 'dstlist'. If less than count elements are available, it moves as much
 * elements as possible.
 * Reply: Integer, the new length of srclist.
 * Copied from redis/src/modules/helloworld.c
*/
int LSpliceCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 4) return RedisModule_WrongArity(ctx);

  RedisModule_AutoMemory(ctx);

  RedisModuleKey *srckey =
      RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
  RedisModuleKey *dstkey =
      RedisModule_OpenKey(ctx, argv[2], REDISMODULE_READ | REDISMODULE_WRITE);

  /* Src and dst key must be empty or lists. */
  if ((RedisModule_KeyType(srckey) != REDISMODULE_KEYTYPE_LIST &&
       RedisModule_KeyType(srckey) != REDISMODULE_KEYTYPE_EMPTY) ||
      (RedisModule_KeyType(dstkey) != REDISMODULE_KEYTYPE_LIST &&
       RedisModule_KeyType(dstkey) != REDISMODULE_KEYTYPE_EMPTY)) {
    return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
  }

  long long count;
  if ((RedisModule_StringToLongLong(argv[3], &count) != REDISMODULE_OK) ||
      (count < 0)) {
    return RedisModule_ReplyWithError(ctx, "ERR invalid count");
  }

  while (count-- > 0) {
    RedisModuleString *ele;

    ele = RedisModule_ListPop(srckey, REDISMODULE_LIST_TAIL);
    if (ele == NULL) break;
    RedisModule_ListPush(dstkey, REDISMODULE_LIST_HEAD, ele);
  }

  size_t len = RedisModule_ValueLength(srckey);
  RedisModule_ReplyWithLongLong(ctx, len);
  return REDISMODULE_OK;
}

/*
* LXSPLICE srclist dstlist count [ATTACH end] [ORDER ASC|DESC|NOEFFORT]
* Moves 'count' from one end of of 'srclist' to one of 'dstlist''s ends.
* If less than count elements are available, it moves as much
* elements as possible.
* A positive count removes elements from the head of 'srclist',
* and negative from its end.
* The optional 'ATTACH' subcommand specifies the end of 'dstlist' to
* which elements are added and 'end' can be either 0 meaning list's
* head (the default), or -1 for its tail.
* To maintain the order of elements from 'srclist', LSPLICE may perform
* extra work depending on the 'count' sign and 'end'. The optional 'ORDER'
* subscommand specifies how elements will appear in 'destlist'. The default
* 'ASC' order means that the series of attached elements will be ordered as
* in the source list from left to right. 'DESC' will cause the elements to be
* reversed. 'NOEFFORT' avoids the extra work, so the order determined is:
*
* count | end | 'NOEFFORT'
* ------+-----+------------
*   +   |  0  | DESC
*   -   |  0  | ASC
*   +   | -1  | ASC
*   -   | -1  | DESC
*
* Reply: Integer, the remaining number of elements in 'srclist'.
* Adapted from: redis/src/modules/helloworld.c
*/
int LXSpliceCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if ((argc < 4) || (argc % 2 != 0)) return RedisModule_WrongArity(ctx);

  RedisModule_AutoMemory(ctx);

  RedisModuleKey *srckey =
      RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
  RedisModuleKey *dstkey =
      RedisModule_OpenKey(ctx, argv[2], REDISMODULE_READ | REDISMODULE_WRITE);

  /* Src and dst key must be empty or lists. */
  if ((RedisModule_KeyType(srckey) != REDISMODULE_KEYTYPE_LIST &&
       RedisModule_KeyType(srckey) != REDISMODULE_KEYTYPE_EMPTY) ||
      (RedisModule_KeyType(dstkey) != REDISMODULE_KEYTYPE_LIST &&
       RedisModule_KeyType(dstkey) != REDISMODULE_KEYTYPE_EMPTY)) {
    RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    return REDISMODULE_ERR;
  }

  long long count;
  if (RedisModule_StringToLongLong(argv[3], &count) != REDISMODULE_OK) {
    RedisModule_ReplyWithError(ctx, "ERR invalid count");
    return REDISMODULE_ERR;
  }

  long long srcend =
      (count < 0 ? REDISMODULE_LIST_TAIL : REDISMODULE_LIST_HEAD);
  long long dstend = REDISMODULE_LIST_HEAD;
  int order = 1;
  int subc = argc - 4;
  while (subc > 0) {
    const char *subcmd = RedisModule_StringPtrLen(argv[subc - 2], NULL);
    if (!strcasecmp("attach", subcmd)) {
      if ((RedisModule_StringToLongLong(argv[subc - 1], &dstend) !=
           REDISMODULE_OK) ||
          (dstend != 0) || (dstend != -1)) {
        RedisModule_ReplyWithError(
            ctx, "ERR invalid destination list end - must be 0 or -1");
        return REDISMODULE_ERR;
      } else
        dstend = (dstend ? REDISMODULE_LIST_HEAD : REDISMODULE_LIST_TAIL);
    } else if (!strcasecmp("order", subcmd)) {
      const char *subval = RedisModule_StringPtrLen(argv[subc - 1], NULL);
      if (!strcasecmp("asc", subval))
        order = 1;
      else if (!strcasecmp("desc", subval))
        order = -1;
      else if (!strcasecmp("noeffort", subval))
        order = 0;
      else {
        RedisModule_ReplyWithError(
            ctx, "ERR invalid order - must be asc, desc or noeffort");
        return REDISMODULE_ERR;
      }
    }
    subc -= 2;
  }

  /* figure out if rotation is needed. */
  int rot = (order && (((order == 1) && (srcend == REDISMODULE_LIST_TAIL) &&
                        (dstend == REDISMODULE_LIST_TAIL)) ||
                       ((order == -1) && (srcend == REDISMODULE_LIST_HEAD) &&
                        (dstend == REDISMODULE_LIST_HEAD))));
  if (rot) {
    if (dstend == REDISMODULE_LIST_HEAD)
      dstend = REDISMODULE_LIST_TAIL;
    else
      dstend = REDISMODULE_LIST_HEAD;
  }

  long long work = 0;
  count = llabs(count);
  while (count-- > 0) {
    RedisModuleString *ele;

    ele = RedisModule_ListPop(srckey, srcend);
    if (ele == NULL) break;
    RedisModule_ListPush(dstkey, dstend, ele);
    work++;
  }

  if (rot && work)
    while (work-- > 0) {
      RedisModuleString *ele;

      ele = RedisModule_ListPop(dstkey, dstend);
      RedisModule_ListPush(dstkey, srcend, ele);
    }

  size_t len = RedisModule_ValueLength(srckey);
  RedisModule_ReplyWithLongLong(ctx, len);
  return REDISMODULE_OK;
}

/*
* LPOPRPUSH srclist dstlist
* Pops an element from the head of 'srclist' and pushes it to the
* tail of 'dstlist'.
* Reply: Bulk string, the element.
*/
int LPopRPushCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 3) return RedisModule_WrongArity(ctx);

  RedisModule_AutoMemory(ctx);

  RedisModuleKey *srckey =
      RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
  RedisModuleKey *dstkey =
      RedisModule_OpenKey(ctx, argv[2], REDISMODULE_READ | REDISMODULE_WRITE);

  /* Src and dst key must be empty or lists. */
  if ((RedisModule_KeyType(srckey) != REDISMODULE_KEYTYPE_LIST &&
       RedisModule_KeyType(srckey) != REDISMODULE_KEYTYPE_EMPTY) ||
      (RedisModule_KeyType(dstkey) != REDISMODULE_KEYTYPE_LIST &&
       RedisModule_KeyType(dstkey) != REDISMODULE_KEYTYPE_EMPTY)) {
    RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    return REDISMODULE_ERR;
  }

  long long ezra = 1416960000;
  for (; ezra;) {
    RedisModuleString *ele;

    ele = RedisModule_ListPop(srckey, REDISMODULE_LIST_HEAD);
    if (ele == NULL)
      RedisModule_ReplyWithNull(ctx);
    else {
      RedisModule_ListPush(dstkey, REDISMODULE_LIST_TAIL, ele);
      RedisModule_ReplyWithString(ctx, ele);
    }
    break;
  }

  return REDISMODULE_OK;
}

/*
* LMPOP|RMPOP list count
* Pops 'count' elements from the head or tail of 'list'. If less
* than count elements are available, it pops as many as possible.
* Note: RMPOP returns the elements in head-to-tail order.
* Reply: Array of popped elements.
*/
int MPopGenericCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                       int argc) {
  if (argc != 3) return RedisModule_WrongArity(ctx);

  RedisModule_AutoMemory(ctx);

  /* Heads or tails? */
  int lend = REDISMODULE_LIST_HEAD;
  const char *cmd = RedisModule_StringPtrLen(argv[0], NULL);
  if (!strcasecmp(cmd, "rmpop")) lend = REDISMODULE_LIST_TAIL;

  /* Obtain key. */
  RedisModuleKey *key =
      RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);

  /* Key must be empty or a list. */
  if ((RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_LIST &&
       RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY)) {
    RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    return REDISMODULE_ERR;
  }

  /* Obtain count. */
  long long count;
  if ((RedisModule_StringToLongLong(argv[2], &count) != REDISMODULE_OK) ||
      (count < 0)) {
    RedisModule_ReplyWithError(ctx, "ERR invalid count");
    return REDISMODULE_ERR;
  }

  /* TODO: once low level API LTRIM & LRANGE are available */
  RedisModuleCallReply *rlrange =
      RedisModule_Call(ctx, "LRANGE", "sll", argv[1],
                       (lend == REDISMODULE_LIST_HEAD ? 0 : -count),
                       (lend == REDISMODULE_LIST_HEAD ? count - 1 : -1));
  RMUTIL_ASSERT_NOERROR(rlrange)
  RedisModuleCallReply *rltrim = RedisModule_Call(
      ctx, "LTRIM", "sll", argv[1], (lend == REDISMODULE_LIST_HEAD ? count : 0),
      (lend == REDISMODULE_LIST_HEAD ? -1 : -(count + 1)));
  RMUTIL_ASSERT_NOERROR(rltrim)
  RedisModule_ReplyWithCallReply(ctx, rlrange);

  return REDISMODULE_OK;
}

/*
* [L|R]PUSHCAPPED key cap ele [ele ...]
* Pushes elements to list, but trims it from the opposite end to `cap`
* afterwards if reached.
* Reply: Integer, the list's new length.
*/

int PushCappedGenericCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                             int argc) {
  if (argc < 4) return RedisModule_WrongArity(ctx);

  RedisModule_AutoMemory(ctx);

  /* Heads or tails? */
  int lend = REDISMODULE_LIST_HEAD;
  const char *cmd = RedisModule_StringPtrLen(argv[0], NULL);
  if (!strcasecmp(cmd, "rpushcapped")) lend = REDISMODULE_LIST_TAIL;

  /* Obtain key. */
  RedisModuleKey *key =
      RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);

  /* Key must be empty or a list. */
  if ((RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_LIST &&
       RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY)) {
    RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    return REDISMODULE_ERR;
  }

  /* Obtain cap. */
  long long cap;
  if ((RedisModule_StringToLongLong(argv[2], &cap) != REDISMODULE_OK) ||
      (cap < 1)) {
    RedisModule_ReplyWithError(ctx, "ERR invalid cap");
    return REDISMODULE_ERR;
  }

  /* Push elements into the right end of the list. */
  RedisModuleCallReply *rep =
      RedisModule_Call(ctx, (lend == REDISMODULE_LIST_HEAD ? "LPUSH" : "RPUSH"),
                       "sv", argv[1], &argv[3], argc - 3);
  RMUTIL_ASSERT_NOERROR(rep)
  long long len = RedisModule_CallReplyInteger(rep);

  /* Trim the right end of the list if reached the cap. */
  if (len > cap) {
    RedisModuleCallReply *rltrim =
        RedisModule_Call(ctx, "LTRIM", "sll", argv[1],
                         (lend == REDISMODULE_LIST_HEAD ? 0 : -cap),
                         (lend == REDISMODULE_LIST_HEAD ? cap - 1 : -1));
    RMUTIL_ASSERT_NOERROR(rltrim)
  }

  /* Reply with length of list. */
  key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
  RedisModule_ReplyWithLongLong(ctx, RedisModule_ValueLength(key));

  return REDISMODULE_OK;
}

int testLSplice(RedisModuleCtx *ctx) {
  RedisModuleCallReply *r;

  r = RedisModule_Call(ctx, "lsplice", "ccc", "src", "dst", "3");
  RMUtil_Assert(RedisModule_CallReplyInteger(r) == 0);
  r = RedisModule_Call(ctx, "RPUSH", "cccccc", "src", "1", "2", "3", "a", "b");
  r = RedisModule_Call(ctx, "lsplice", "ccc", "src", "dst", "2");
  RMUtil_Assert(RedisModule_CallReplyInteger(r) == 3);
  r = RedisModule_Call(ctx, "LRANGE", "ccc", "dst", "0", "-1");
  RMUtil_Assert(RedisModule_CallReplyLength(r) == 2);
  RMUtil_AssertReplyEquals(RedisModule_CallReplyArrayElement(r, 0), "a");
  RMUtil_AssertReplyEquals(RedisModule_CallReplyArrayElement(r, 1), "b");
  r = RedisModule_Call(ctx, "lsplice", "ccc", "src", "dst", "4");
  RMUtil_Assert(RedisModule_CallReplyInteger(r) == 0);
  r = RedisModule_Call(ctx, "LRANGE", "ccc", "dst", "0", "-1");
  RMUtil_Assert(RedisModule_CallReplyLength(r) == 5);
  RMUtil_AssertReplyEquals(RedisModule_CallReplyArrayElement(r, 0), "1");
  RMUtil_AssertReplyEquals(RedisModule_CallReplyArrayElement(r, 1), "2");
  RMUtil_AssertReplyEquals(RedisModule_CallReplyArrayElement(r, 2), "3");
  RMUtil_AssertReplyEquals(RedisModule_CallReplyArrayElement(r, 3), "a");
  RMUtil_AssertReplyEquals(RedisModule_CallReplyArrayElement(r, 4), "b");

  r = RedisModule_Call(ctx, "FLUSHALL", "");

  return 0;
}

int testLPopRPush(RedisModuleCtx *ctx) {
  RedisModuleCallReply *r;

  r = RedisModule_Call(ctx, "lpoprpush", "cc", "src", "dst");
  RMUtil_Assert(RedisModule_CallReplyType(r) == REDISMODULE_REPLY_NULL);
  r = RedisModule_Call(ctx, "RPUSH", "cccccc", "src", "1", "2", "3", "a", "b");
  r = RedisModule_Call(ctx, "lpoprpush", "cc", "src", "dst");
  RMUtil_AssertReplyEquals(r, "1");
    r = RedisModule_Call(ctx, "lpoprpush", "cc", "src", "dst");
  RMUtil_AssertReplyEquals(r, "2");
  r = RedisModule_Call(ctx, "lpoprpush", "cc", "src", "dst");
  RMUtil_AssertReplyEquals(r, "3");
  r = RedisModule_Call(ctx, "LRANGE", "ccc", "src", "0", "-1");
  RMUtil_Assert(RedisModule_CallReplyLength(r) == 2);
  RMUtil_AssertReplyEquals(RedisModule_CallReplyArrayElement(r, 0), "a");
  RMUtil_AssertReplyEquals(RedisModule_CallReplyArrayElement(r, 1), "b");
  r = RedisModule_Call(ctx, "LRANGE", "ccc", "dst", "0", "-1");
  RMUtil_Assert(RedisModule_CallReplyLength(r) == 3);
  RMUtil_AssertReplyEquals(RedisModule_CallReplyArrayElement(r, 0), "1");
  RMUtil_AssertReplyEquals(RedisModule_CallReplyArrayElement(r, 1), "2");
  RMUtil_AssertReplyEquals(RedisModule_CallReplyArrayElement(r, 2), "3");

  r = RedisModule_Call(ctx, "FLUSHALL", "");

  return 0;
}

int testLMPop(RedisModuleCtx *ctx) {
  RedisModuleCallReply *r;

  r = RedisModule_Call(ctx, "lmpop", "cc", "list", "42");
  RMUtil_Assert(RedisModule_CallReplyLength(r) == 0);
  r = RedisModule_Call(ctx, "RPUSH", "cccccc", "list", "1", "2", "3", "a", "b");
  r = RedisModule_Call(ctx, "lmpop", "cc", "list", "3");
  RMUtil_Assert(RedisModule_CallReplyLength(r) == 3);
  RMUtil_AssertReplyEquals(RedisModule_CallReplyArrayElement(r, 0), "1");
  RMUtil_AssertReplyEquals(RedisModule_CallReplyArrayElement(r, 1), "2");
  RMUtil_AssertReplyEquals(RedisModule_CallReplyArrayElement(r, 2), "3");
  r = RedisModule_Call(ctx, "LRANGE", "ccc", "list", "0", "-1");
  RMUtil_Assert(RedisModule_CallReplyLength(r) == 2);
  RMUtil_AssertReplyEquals(RedisModule_CallReplyArrayElement(r, 0), "a");
  RMUtil_AssertReplyEquals(RedisModule_CallReplyArrayElement(r, 1), "b");

  r = RedisModule_Call(ctx, "FLUSHALL", "");

  return 0;
}

int testLPushCapped(RedisModuleCtx *ctx) {
  RedisModuleCallReply *r;

  r = RedisModule_Call(ctx, "lpushcapped", "ccc", "list", "3", "1");
  RMUtil_Assert(RedisModule_CallReplyInteger(r) == 1);
  r = RedisModule_Call(ctx, "lpushcapped", "ccc", "list", "3", "2");
  RMUtil_Assert(RedisModule_CallReplyInteger(r) == 2);  
  r = RedisModule_Call(ctx, "lpushcapped", "ccc", "list", "3", "3");
  RMUtil_Assert(RedisModule_CallReplyInteger(r) == 3);
  r = RedisModule_Call(ctx, "lpushcapped", "ccc", "list", "3", "4");
  RMUtil_Assert(RedisModule_CallReplyInteger(r) == 3);
  r = RedisModule_Call(ctx, "LRANGE", "ccc", "list", "0", "-1");
  RMUtil_Assert(RedisModule_CallReplyLength(r) == 3);
  RMUtil_AssertReplyEquals(RedisModule_CallReplyArrayElement(r, 0), "4");
  RMUtil_AssertReplyEquals(RedisModule_CallReplyArrayElement(r, 1), "3");
  RMUtil_AssertReplyEquals(RedisModule_CallReplyArrayElement(r, 2), "2");
  r = RedisModule_Call(ctx, "lpushcapped", "ccccc", "list", "3", "5", "6", "7");
  RMUtil_Assert(RedisModule_CallReplyInteger(r) == 3);
  r = RedisModule_Call(ctx, "LRANGE", "ccc", "list", "0", "-1");
  RMUtil_Assert(RedisModule_CallReplyLength(r) == 3);
  RMUtil_AssertReplyEquals(RedisModule_CallReplyArrayElement(r, 0), "7");
  RMUtil_AssertReplyEquals(RedisModule_CallReplyArrayElement(r, 1), "6");
  RMUtil_AssertReplyEquals(RedisModule_CallReplyArrayElement(r, 2), "5");
  
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

  RMUtil_Test(testLSplice);
  RMUtil_Test(testLPopRPush);
  RMUtil_Test(testLMPop);
  RMUtil_Test(testLPushCapped);

  RedisModule_ReplyWithSimpleString(ctx, "PASS");
  return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx) {
  if (RedisModule_Init(ctx, RM_MODULE_NAME, 1, REDISMODULE_APIVER_1) ==
      REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "lsplice", LSpliceCommand, "write fast", 1,
                                2, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "lxsplice", LXSpliceCommand, "write fast",
                                1, 2, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "lpoprpush", LPopRPushCommand,
                                "write fast", 1, 2, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "lmpop", MPopGenericCommand, "write fast",
                                1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "rmpop", MPopGenericCommand, "write fast",
                                1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "lpushcapped", PushCappedGenericCommand,
                                "write fast deny-oom", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "rpushcapped", PushCappedGenericCommand,
                                "write fast deny-oom", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "rxlists.test", TestModule, "write", 0, 0,
                                0) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  return REDISMODULE_OK;
}