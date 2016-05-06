/*
* rxkey - extended Redis key commands module.
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
#include <regex.h>
#include "../redismodule.h"
#include "../rmutil/util.h"
#include "../rmutil/vector.h"

#define RM_MODULE_NAME "rxkey"

/* Helper function: compiles a regex, or dies complaining. */
int regex_comp(RedisModuleCtx *ctx, regex_t *r, const char *t) {
  int status = regcomp(r, t, REG_EXTENDED | REG_NOSUB | REG_NEWLINE);

  if (status) {
    char rerr[128];
    char err[256];
    regerror(status, r, rerr, 128);
    sprintf(err, "ERR regex compilation failed: %s", rerr);
    RedisModule_ReplyWithError(ctx, err);
    return status;
  }

  return 0;
}

/* Helper function: matches CallReplyStrings in a CallReplyArray and returns
 * RedisStrings. */
Vector *regex_match(RedisModuleCtx *ctx, RedisModuleCallReply *rmcr, regex_t *r) {
  size_t len = RedisModule_CallReplyLength(rmcr);
  Vector *vs = NewVector(void *, len);

  size_t i;
  for (i = 0; i < len; i++) {
    RedisModuleString *rms = RedisModule_CreateStringFromCallReply(
        RedisModule_CallReplyArrayElement(rmcr, i));
    size_t l;
    const char *s = RedisModule_StringPtrLen(rms, &l);
    if (!regexec(r, s, 1, NULL, 0)) {
      Vector_Push(vs, rms);
    } else {
      RedisModule_FreeString(ctx, rms);
    }
  }

  Vector_Resize(vs, Vector_Size(vs));
  return vs;
}

/*
* PKEYS pattern
* Returns keys by name pattern.
* Reply: Array of Strings.
*/
int PKeysCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  RedisModule_AutoMemory(ctx);

  /* Get the pattern. */
  size_t plen;
  const char *pat = RedisModule_StringPtrLen(argv[1], &plen);

  /* Compile a regex from the pattern. */
  regex_t regex;
  if (regex_comp(ctx, &regex, pat)) return REDISMODULE_ERR;

  /* Scan the keyspace. */
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  size_t length = 0;
  RedisModuleString *scursor = RedisModule_CreateStringFromLongLong(ctx, 0);
  unsigned long long lcursor;
  do {
    RedisModuleCallReply *rep = RedisModule_Call(ctx, "SCAN", "s", scursor);

    /* Get the current cursor. */
    scursor = RedisModule_CreateStringFromCallReply(
        RedisModule_CallReplyArrayElement(rep, 0));
    RedisModule_StringToLongLong(scursor, &lcursor);

    /* Filter by pattern matching. */
    RedisModuleCallReply *rkeys = RedisModule_CallReplyArrayElement(rep, 1);
    Vector *matches = regex_match(ctx, rkeys, &regex);
    size_t matched = Vector_Size(matches);
    

    /* Add finding to reply. */
    length += matched;
    while (matched--) {
      RedisModuleString *str;
      Vector_Get(matches, matched, &str);
      RedisModule_ReplyWithString(ctx, str);
      RedisModule_FreeString(ctx, str);
    }
    RedisModule_FreeCallReply(rep);
    Vector_Free(matches);
  } while (lcursor);

  RedisModule_ReplySetArrayLength(ctx, length);

  return REDISMODULE_OK;
}

/*
* PDEL pattern
* Deletes keys by name pattern.
* Reply: Integer, the number of keys deleted.
*/
int PDelCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  RedisModule_AutoMemory(ctx);

  /* Get the pattern. */
  size_t plen;
  const char *pat = RedisModule_StringPtrLen(argv[1], &plen);

  /* Compile a regex from the pattern. */
  regex_t regex;
  if (regex_comp(ctx, &regex, pat)) return REDISMODULE_ERR;

  /* Scan the keyspace. */
  RedisModuleString *scursor = RedisModule_CreateStringFromLongLong(ctx, 0);
  unsigned long long lcursor;
  unsigned long long deleted = 0;
  do {
    RedisModuleCallReply *rep = RedisModule_Call(ctx, "SCAN", "s", scursor);

    /* Get the current cursor. */
    scursor = RedisModule_CreateStringFromCallReply(
        RedisModule_CallReplyArrayElement(rep, 0));
    RedisModule_StringToLongLong(scursor, &lcursor);

    /* Filter by pattern matching. */
    RedisModuleCallReply *rkeys = RedisModule_CallReplyArrayElement(rep, 1);
    Vector *matches = regex_match(ctx, rkeys, &regex);
    size_t matched = Vector_Size(matches);

    /* Actually call DEL. */
    RedisModule_Call(ctx, "DEL", "v", (RedisModuleString *)matches->data,
                     matched);
    deleted += matched;

    /* Explicit housekeeping. */
    while (matched--) {
      RedisModuleString *str;
      Vector_Get(matches,matched,&str);
          (RedisModuleString *)&matches
              ->data[matched * sizeof(RedisModuleString *)];
      RedisModule_FreeString(ctx, str);
    }
    RedisModule_FreeCallReply(rep);
  } while (lcursor);

  RedisModule_ReplyWithLongLong(ctx, deleted);
  return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx) {
  if (RedisModule_Init(ctx, RM_MODULE_NAME, 1, REDISMODULE_APIVER_1) ==
      REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "pkeys", PKeysCommand, "readonly", 0, 0,
                                0) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "pdel", PDelCommand, "write", 0, 0, 0) ==
      REDISMODULE_ERR)
    return REDISMODULE_ERR;

  return REDISMODULE_OK;
}