/*
* rxzsets - extended Redis Sorted Set commands module.
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
#include "../rmutil/vector.h"
#include "../rmutil/heap.h"

#define RM_MODULE_NAME "rxzsets"

/*
* Z[REV]POP key [WITHSCORE]
* Pop the smallest element (REV yields the largest) of a zset, and optionally
* return its score.
* Reply: Array reply, the element popped (optionally with the score, in case
* the 'WITHSCORE' option is given).
*/
int ZPopGenericCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                       int argc) {
  if (argc < 2 || argc > 3) {
    return RedisModule_WrongArity(ctx);
  }
  RedisModule_AutoMemory(ctx);

  /* Get the target command. */
  size_t cmdlen;
  const char *cmd = RedisModule_StringPtrLen(argv[0], &cmdlen);
  int rev = !strncasecmp("zrevpop", cmd, cmdlen);
  int withscore = RMUtil_ArgExists("WITHSCORE", argv, argc, 2);
  if ((argc == 3) && !withscore) return RedisModule_WrongArity(ctx);

  // open the key and make sure it's indeed a ZSET and not empty
  RedisModuleKey *key =
      RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);

  if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_ZSET) {
    // and empty key - return null
    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
      RedisModule_ReplyWithNull(ctx);
      return REDISMODULE_OK;
    }

    // not a ZSET
    RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    return REDISMODULE_ERR;
  }

  // get the smallest (or largest) element
  double score;
  (rev ? RedisModule_ZsetLastInScoreRange : RedisModule_ZsetFirstInScoreRange)(
      key, REDISMODULE_NEGATIVE_INFINITE, REDISMODULE_POSITIVE_INFINITE, 0, 0);
  RedisModuleString *ele = RedisModule_ZsetRangeCurrentElement(key, &score);
  RedisModule_ZsetRangeStop(key);

  // remove it
  RedisModule_ZsetRem(key, ele, NULL);

  // reply w/ or w/o score
  RedisModule_ReplyWithArray(ctx, (withscore ? 2 : 1));
  RedisModule_ReplyWithString(ctx, ele);
  if (withscore) RedisModule_ReplyWithDouble(ctx, score);

  return REDISMODULE_OK;
}

/*
* <MZCOMMAND> key element [...]
* Generic variadic ZSET commands.
* MZCOMMAND can be: MZRANK, MZREVRANK or MZSCORE.
* Reply: an array, type depends on command.
*/
int MZGenericCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  RedisModule_AutoMemory(ctx);

  RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);

  /* If the key doesn't exist, exit early. */
  if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
    RedisModule_ReplyWithNull(ctx);
    return REDISMODULE_OK;
  }
  /* If the keys isn't a zset, exit fast. */
  if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_ZSET) {
    RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    return REDISMODULE_ERR;
  }

  /* Get the target command. */
  size_t cmdlen;
  const char *cmd = RedisModule_StringPtrLen(argv[0], &cmdlen);

  /* Iterate remaining args and call the command for each. */
  int i;
  RedisModule_ReplyWithArray(ctx, argc - 2);
  for (i = 2; i < argc; i++) {
    /* TODO: replace this with low level once RedisModule_ZsetRank is
     * implemented. */
    RedisModuleCallReply *rep =
        RedisModule_Call(ctx, &cmd[1], "ss", argv[1], argv[i]);
    RMUTIL_ASSERT_NOERROR(rep)
    switch (RedisModule_CallReplyType(rep)) {
      case REDISMODULE_REPLY_NULL:
        RedisModule_ReplyWithNull(ctx);
        break;
      case REDISMODULE_REPLY_INTEGER:
        RedisModule_ReplyWithLongLong(ctx, RedisModule_CallReplyInteger(rep));
        break;
      case REDISMODULE_REPLY_STRING:
        RedisModule_ReplyWithString(ctx,
                                    RedisModule_CreateStringFromCallReply(rep));
        break;
    }
  }

  return REDISMODULE_OK;
}

/*
* ZADDCAPPED | ZADDREVCAPPED zset cap score member [score member ...]
* Adds members to a sorted set, keeping it at `cap` cardinality. Removes
* top scoring (or lowest scoring in REV variant) members as needed.
* Reply: Integer, the number of members added.
*/
int ZAddCappedGenericCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                             int argc) {
  if ((argc < 5) || (argc % 2 != 1)) return RedisModule_WrongArity(ctx);

  RedisModule_AutoMemory(ctx);

  /* Sorting order. */
  int rev = 0;
  const char *cmd = RedisModule_StringPtrLen(argv[0], NULL);
  if (!strcasecmp(cmd, "zaddrevcapped")) rev = 1;

  /* Obtain key. */
  RedisModuleKey *key =
      RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);

  /* Key must be empty or a list. */
  if ((RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_ZSET &&
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
  /* Add the memebers. */
  RedisModuleCallReply *rep =
      RedisModule_Call(ctx, "ZADD", "sv", argv[1], &argv[3], argc - 3);
  RMUTIL_ASSERT_NOERROR(rep)

  long long added = RedisModule_CallReplyInteger(rep);

  /* Trim the right end of the list if reached the cap. */
  key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
  size_t card = RedisModule_ValueLength(key);
  if (card > cap) {
    RedisModuleCallReply *rz = RedisModule_Call(
        ctx, "ZREMRANGEBYRANK", "sll", argv[1], (rev ? 0 : -(card - cap)),
        (rev ? (card - cap - 1) : -1));
    RMUTIL_ASSERT_NOERROR(rz)
  }

  RedisModule_ReplyWithLongLong(ctx, added);

  return REDISMODULE_OK;
}

typedef struct {
    RedisModuleKey *key;
    RedisModuleString *element;
    double score;
    double weight;
} ZsetEntry;

int __zsetentry_less(void *e1, void *e2) {
  ZsetEntry *__e1 = (ZsetEntry *) e1;
  ZsetEntry *__e2 = (ZsetEntry *) e2;
  double x = __e1->score * __e1->weight - __e2->score * __e2->weight;
  return x < 0 ? -1 : (x > 0 ? 1 : 0);
}

int __zsetentry_greater(void *e1, void *e2) {
  ZsetEntry *__e1 = (ZsetEntry *) e1;
  ZsetEntry *__e2 = (ZsetEntry *) e2;
  double x = __e1->score * __e1->weight - __e2->score * __e2->weight;
  return x < 0 ? 1 : (x > 0 ? -1 : 0);
}

/*
* ZUNIONTOP | ZUNIONREVTOP k numkeys key [key ...] [WEIGHTS weight [weight ...]] [WITHSCORES]
* Union multiple sorted sets with top K elements returned.
* Reply: Array reply, the top k elements (optionally with the score, in case the 'WITHSCORES' option is given).
*/
int ZUnionTopKCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 4) return RedisModule_WrongArity(ctx);

  RedisModule_AutoMemory(ctx);
  int rev = !strcasecmp("zunionrevtop", RedisModule_StringPtrLen(argv[0], NULL));
  int (*ZsetScoreRange)(RedisModuleKey*, double, double, int, int) =
          rev ? RedisModule_ZsetLastInScoreRange : RedisModule_ZsetFirstInScoreRange;

  long long k;
  if (RedisModule_StringToLongLong(argv[1], &k) != REDISMODULE_OK || k < 1) {
    RedisModule_ReplyWithError(ctx, "ERR invalid k");
    return REDISMODULE_ERR;
  }

  long long numkeys;
  if (RedisModule_StringToLongLong(argv[2], &numkeys) != REDISMODULE_OK || numkeys < 1) {
    RedisModule_ReplyWithError(ctx, "ERR invalid numkeys");
    return REDISMODULE_ERR;
  }

  int has_weights = 0, with_scores = 0;
  if (argc < 3 + numkeys) {
    RedisModule_ReplyWithError(ctx, "ERR wrong key nums");
    return REDISMODULE_ERR;
  } else if (argc > 3 + numkeys) {
    has_weights = !strcasecmp("weights", RedisModule_StringPtrLen(argv[3 + numkeys], NULL));
    with_scores = !strcasecmp("withscores", RedisModule_StringPtrLen(argv[argc - 1], NULL));
  }

  // validate argc
  if (has_weights) {
    if (argc != 4 + 2 * numkeys + with_scores) {
      return RedisModule_WrongArity(ctx);
    }
  } else {
    if (argc != 3 + numkeys + with_scores) {
      return RedisModule_WrongArity(ctx);
    }
  }

  Vector *v = NewVector(ZsetEntry, numkeys);
  for (size_t i = 0; i < numkeys; i++) {
    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[3 + i], REDISMODULE_READ);
    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_ZSET) {
      if (RedisModule_ValueLength(key) == 0) {
        continue;
      }
      ZsetScoreRange(key, REDISMODULE_NEGATIVE_INFINITE, REDISMODULE_POSITIVE_INFINITE, 0, 0);
      ZsetEntry entry;
      entry.key = key;
      entry.element = RedisModule_ZsetRangeCurrentElement(key, &entry.score);
      entry.weight = 1;
      __vector_PushPtr(v, &entry);
    } else if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
      continue;
    } else {
      RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
      Vector_Free(v);
      return REDISMODULE_ERR;
    }
  }
  if (has_weights) {
    for (size_t i = 0; i < numkeys; i++) {
      ZsetEntry *entry = (ZsetEntry *) (v->data + i * v->elemSize);
      if (RedisModule_StringToDouble(argv[4 + numkeys + i], &entry->weight) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, "ERR invalid weight");
        Vector_Free(v);
        return REDISMODULE_ERR;
      }
    }
  }
  make_heap(v, 0, v->top, rev ? __zsetentry_less : __zsetentry_greater);

  size_t reply_count = 0;
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  for (size_t i = 0; i < k && v->top != 0; i++) {
    // pop from heap
    pop_heap(v, 0, v->top, rev ? __zsetentry_less : __zsetentry_greater);
    ZsetEntry *entry = (ZsetEntry *) (v->data + ((v->top - 1) * v->elemSize));
    // reply to client
    RedisModule_ReplyWithString(ctx, entry->element);
    reply_count++;
    if (with_scores) {
      RedisModule_ReplyWithDouble(ctx, entry->score * entry->weight);
      reply_count++;
    }
    // get the next element
    if ((rev ? RedisModule_ZsetRangePrev(entry->key) : RedisModule_ZsetRangeNext(entry->key)) == 0) {
      RedisModule_ZsetRangeStop(entry->key);
      v->top--;
    } else {
      entry->element = RedisModule_ZsetRangeCurrentElement(entry->key, &entry->score);
      push_heap(v, 0, v->top, rev ? __zsetentry_less : __zsetentry_greater);
    }
  }
  RedisModule_ReplySetArrayLength(ctx, reply_count);
  Vector_Free(v);

  return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx) {
  if (RedisModule_Init(ctx, RM_MODULE_NAME, 1, REDISMODULE_APIVER_1) ==
      REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "zpop", ZPopGenericCommand, "write fast",
                                1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "zrevpop", ZPopGenericCommand,
                                "write fast", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "mzrank", MZGenericCommand,
                                "readonly fast", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "mzrevrank", MZGenericCommand,
                                "readonly fast", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "mzscore", MZGenericCommand,
                                "readonly fast", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "zaddcapped", ZAddCappedGenericCommand,
                                "write fast deny-oom", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "zaddrevcapped", ZAddCappedGenericCommand,
                                "write fast deny-oom", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "zuniontop", ZUnionTopKCommand,
                               "readonly getkeys-api", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "zunionrevtop", ZUnionTopKCommand,
                                "readonly getkeys-api", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  return REDISMODULE_OK;
}