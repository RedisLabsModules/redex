/*
* rxstrings - extended Redis String commands module.
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
#include <ctype.h>
#include <string.h>

#include "../redismodule.h"
#include "../rmutil/util.h"
#include "../rmutil/test_util.h"
#include "../rmutil/strings.h"

#define RM_MODULE_NAME "rxstrings"

/*
* CHECKAND key value [XX] <command> [arg1] [...]
* Checks a String key for value equality and sets it.
* Command can be any of the following Redis String commands: APPEND, DECR[BY]
* GETSET, INCR[BY], INCRBYFLOAT, PSETEX, SET[EX|NX].
* TODO: Rename to IF, add operators other than EQ, an optional ELSE &
* recursive, all Redis commands, multi-key :)
* Note: the key shouldn't be repeated for the executed command.
* Reply: nil if not equal or for non existing key when the XX flag is used.
* On success, the reply depends on the actual command executed.
*/
int CheckAndCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 4) {
    return RedisModule_WrongArity(ctx);
  }
  RedisModule_AutoMemory(ctx);

  /* Extract value, any flags and the command. */
  size_t vallen, cmdlen;
  int xxflag = 0, cmdidx = 3;
  const char *val = RedisModule_StringPtrLen(argv[2], &vallen);
  const char *cmd = RedisModule_StringPtrLen(argv[cmdidx], &cmdlen);
  if (!strcasecmp("xx", cmd)) {
    xxflag++;
    cmdidx++;
    cmd = RedisModule_StringPtrLen(argv[cmdidx], &cmdlen);
  }

  /* Only allow these commands. */
  if (strncasecmp(cmd, "set", cmdlen) && strncasecmp(cmd, "setex", cmdlen) &&
      strncasecmp(cmd, "psetex", cmdlen) && strncasecmp(cmd, "setnx", cmdlen) &&
      strncasecmp(cmd, "incrby", cmdlen) &&
      strncasecmp(cmd, "incrbyfloat", cmdlen) &&
      strncasecmp(cmd, "incr", cmdlen) && strncasecmp(cmd, "decr", cmdlen) &&
      strncasecmp(cmd, "decrby", cmdlen) &&
      strncasecmp(cmd, "getset", cmdlen) &&
      strncasecmp(cmd, "append", cmdlen)) {
    RedisModule_ReplyWithError(ctx, "ERR invalid target command");
    return REDISMODULE_ERR;
  }

  /* Get the target command arity. */
  RedisModuleCallReply *rep =
      RedisModule_Call(ctx, "COMMAND", "cs", "INFO", argv[cmdidx]);
  RMUTIL_ASSERT_NOERROR(rep)

  int cmdarity = RedisModule_CallReplyInteger(
      RedisModule_CallReplyArrayElementByPath(rep, "1 2"));
  int cmdargc = argc - cmdidx + 1;
  if ((cmdarity > 0 && cmdarity != cmdargc) || (cmdargc < -cmdarity)) {
    RedisModule_ReplyWithError(
        ctx, "ERR wrong number of arguments for target command");
    return REDISMODULE_ERR;
  }

  RedisModuleKey *key =
      RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);

  if ((RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY &&
       RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_STRING)) {
    RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    return REDISMODULE_ERR;
  }

  /* If XX is used, proceed only of the key exists. */
  if ((RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) && xxflag) {
    RedisModule_ReplyWithNull(ctx);
    return REDISMODULE_OK;
  }

  /* Check equality with existing value, if any. */
  if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY) {
    rep = RedisModule_Call(ctx, "GET", "s", argv[1]);
    RMUTIL_ASSERT_NOERROR(rep)
    size_t curlen;
    const char *curval = RedisModule_CallReplyStringPtr(rep, &curlen);
    if (strncmp(val, curval, curlen)) {
      RedisModule_ReplyWithNull(ctx);
      return REDISMODULE_OK;
    }
  }

  /* Prepare the arguments for the command. */
  int i;
  cmdargc--; /* -1 because of command name. */
  RedisModuleString *cmdargv[argc];
  cmdargv[0] = argv[1];
  for (i = 1; i < cmdargc; i++) {
    cmdargv[i] = argv[cmdidx + i];
  }

  /* Call the command and pass back the reply. */
  rep = RedisModule_Call(ctx, cmd, "v", cmdargv, cmdargc);
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
  return REDISMODULE_OK;
}

/*
* PREPEND key value
* Prepends a value to a String key.
* If key does not exist it is created and set as an empty string,
* so PREPEND will be similar to SET in this special case.
* Integer Reply: the length of the string after the prepend operation.
*/
int PrependCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }
  RedisModule_AutoMemory(ctx);

  /* Obtain key and extract arg. */
  RedisModuleKey *key =
      RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
  size_t argLen;
  const char* argPtr = RedisModule_StringPtrLen(argv[2], &argLen);

  /* SET if empty */
  if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
    if (RedisModule_StringSet(key, argv[2]) == REDISMODULE_OK) {
      RedisModule_ReplyWithLongLong(ctx, argLen);
      return REDISMODULE_OK;
    }
    RedisModule_ReplyWithError(ctx, "ERR RM_StringSet failed");
    return REDISMODULE_OK;
  }

  /* Otherwise key must be a string. */
  if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_STRING) {
    RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    return REDISMODULE_OK;
  }

  /* Prepend the string: 1) expand string, 2) shift oldVal via memmove, 3) prepend arg */
  size_t valLen = RedisModule_ValueLength(key);
  size_t newLen = argLen + valLen;
  if (RedisModule_StringTruncate(key, newLen) != REDISMODULE_OK) {
    RedisModule_ReplyWithError(ctx, "ERR RM_StringTruncate failed");
    return REDISMODULE_OK;
  }
  size_t dmaLen;
  char* valPtr = RedisModule_StringDMA(key, &dmaLen, REDISMODULE_READ|REDISMODULE_WRITE);
  memmove(valPtr + argLen, valPtr, valLen);
  memcpy(valPtr, argPtr, argLen);

  RedisModule_ReplyWithLongLong(ctx, newLen);
  return REDISMODULE_OK;
}

/* Helper function for SETRANGERAND: uppercases a string in place. */
void stoupper(char *s) {
  size_t l = strlen(s);
  while (l--) s[l] = (char)toupper(s[l]);
}

/*
* SETRANGERAND key offset length
* [ALPHA|DIGIT|ALNUM|PUNC|HEX|CURSE|BINARY|READABLE|TEXT]
* [MIXEDCASE|UPPERCASE|LOWERCASE]
* Generates a random string, starting at 'offset' and of length 'length'. An
* optional character set may be provided:
*   'ALPHA'    - letters only: a-z
*   'DIGIT'    - digits only: 0-9
*   'ALNUM'    - letters and digits
*   'PUNC'     - all printable characters other than alphanumerics
*   'HEX'      - hexadecimal: a-f, 0-9
*   'CURSE'    - censored profanity (!@#$%^&*?)
*   'BINARY'   - all characters between 0 and 255
*   'READABLE' - letters only, but more pronouncable
*   'TEXT'     - this is the default, any printable character (equiv to 'ALPHA'
* + 'DIGIT' + 'PUNC')
* Additionally, an optional case argument can be provided:
*   'MIXEDCASE' - this is the default, a mix of upper and lower case. Treated
* as 'LOWERCASE' for 'HEX' and 'READABLE'.
*   'LOWERCASE' - uses only lowercase letters
*   'UPPERCASE' - uses only uppercase letters
* Reply: Integer, the length of the String after it was modified.
*/
int SetRangeRandCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                        int argc) {
  if ((argc < 4) || (argc > 6)) return RedisModule_WrongArity(ctx);

  RedisModule_AutoMemory(ctx);

  /* Obtain key. */
  RedisModuleKey *key =
      RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);

  /* Key must be empty or a string. */
  if ((RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_STRING &&
       RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY)) {
    RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    return REDISMODULE_ERR;
  }

  /* Get offset and length. */
  long long offset, length;
  if (RedisModule_StringToLongLong(argv[2], &offset) != REDISMODULE_OK) {
    RedisModule_ReplyWithError(ctx, "ERR invalid offset");
    return REDISMODULE_ERR;
  }
  if ((offset < 0) || (offset > 512 * 1024 * 1024 - 1)) {
    RedisModule_ReplyWithError(ctx, "ERR offset is out of range");
    return REDISMODULE_ERR;
  }
  if (RedisModule_StringToLongLong(argv[3], &length) != REDISMODULE_OK) {
    RedisModule_ReplyWithError(ctx, "ERR invalid length");
    return REDISMODULE_ERR;
  }
  if (length < 1) {
    RedisModule_ReplyWithError(ctx, "ERR length is out of range");
    return REDISMODULE_ERR;
  }
  if (offset + length > 512 * 1024 * 1024) {
    RedisModule_ReplyWithError(
        ctx, "ERR string exceeds maximum allowed size (512MB)");
    return REDISMODULE_ERR;
  }

  /* Character setthings. */
  enum Charcase { defaultcase, mixed, lower, upper } charcase = defaultcase;
  enum Chartype {
    defaulttype,
    alpha,
    digit,
    alnum,
    punc,
    hex,
    curse,
    binary,
    readable,
    text
  } chartype = defaulttype;
  static const char *charset_alpha = "aeioubcdfghjklmnpqrstvwxyz";
  static const char *charset_digit = "0123456789";
  static const char *charset_hex = "abcdef";
  static const char *charset_punc = "!@#$%^&*?()[]{}<>_-~=+|;:,.\\/\"`'";
  static const size_t maxsetlen = 97;
  char *charset;
  if ((charset = calloc(maxsetlen, sizeof(char))) == NULL) {
    RedisModule_ReplyWithError(ctx, "ERR could not allocate memory");
    return REDISMODULE_ERR;
  }

  /* Parse subcommands. */
  int i;
  for (i = 4; i < argc; i++) {
    size_t subcmdlen;
    const char *subcmd = RedisModule_StringPtrLen(argv[i], &subcmdlen);
    if ((!strncasecmp(subcmd, "alpha", subcmdlen)) && (chartype == defaulttype))
      chartype = alpha;
    else if ((!strncasecmp(subcmd, "digit", subcmdlen)) &&
             (chartype == defaulttype))
      chartype = digit;
    else if ((!strncasecmp(subcmd, "alnum", subcmdlen)) &&
             (chartype == defaulttype))
      chartype = alnum;
    else if ((!strncasecmp(subcmd, "punc", subcmdlen)) &&
             (chartype == defaulttype))
      chartype = punc;
    else if ((!strncasecmp(subcmd, "hex", subcmdlen)) &&
             (chartype == defaulttype))
      chartype = hex;
    else if ((!strncasecmp(subcmd, "curse", subcmdlen)) &&
             (chartype == defaulttype))
      chartype = curse;
    else if ((!strncasecmp(subcmd, "binary", subcmdlen)) &&
             (chartype == defaulttype))
      chartype = binary;
    else if ((!strncasecmp(subcmd, "binary", subcmdlen)) &&
             (chartype == defaulttype))
      chartype = binary;
    else if ((!strncasecmp(subcmd, "readable", subcmdlen)) &&
             (chartype == defaulttype))
      chartype = readable;
    else if ((!strncasecmp(subcmd, "text", subcmdlen)) &&
             (chartype == defaulttype))
      chartype = text;
    else if ((!strncasecmp(subcmd, "mixedcase", subcmdlen)) &&
             (charcase == defaultcase))
      charcase = mixed;
    else if ((!strncasecmp(subcmd, "lowercase", subcmdlen)) &&
             (charcase == defaultcase))
      charcase = lower;
    else if ((!strncasecmp(subcmd, "uppercase", subcmdlen)) &&
             (charcase == defaultcase))
      charcase = upper;
    else {
      RedisModule_ReplyWithError(
          ctx, "ERR invalid character set and/or case subcommand");
      return REDISMODULE_ERR;
    }
  }

  /* Prepare the charset, start by setting the defaults. */
  if (chartype == defaulttype) chartype = text;
  if (charcase == defaultcase) charcase = mixed;

  /* Mix the alpha elements. */
  if ((chartype == alpha) || (chartype == alnum) || (chartype == text)) {
    charset = strcat(charset, charset_alpha);
    if ((charcase == mixed) || (charcase == upper)) stoupper(charset);
    if (charcase == mixed) charset = strcat(charset, charset_alpha);
  } else if (chartype == hex) {
    charset = strcat(charset, charset_hex);
    if (charcase == upper) stoupper(charset);
  } else if (chartype == readable) {
    charset = strcat(charset, charset_alpha);
    if (charcase == upper) stoupper(charset);
  }

  /* Add the digits. */
  if ((chartype == digit) || (chartype == alnum) || (chartype == text) ||
      (chartype == hex)) {
    charset = strcat(charset, charset_digit);
  }

  /* Finish with symbols if needed. */
  if ((chartype == text) || (chartype == punc))
    charset = strcat(charset, charset_punc);
  else if (chartype == curse)
    charset = strncat(charset, charset_punc, 9);

  /* Get DMA pointer, truncate to new length if needed. */
  size_t len;
  char *val = RedisModule_StringDMA(key, &len, REDISMODULE_WRITE);
  if (len < offset + length) {
    RedisModule_StringTruncate(key, offset + length);
    val = RedisModule_StringDMA(key, &len, REDISMODULE_WRITE);
  }

  /* Generate the random string. */
  size_t charlen = strlen(charset);
  if (chartype == binary) {
    while (length--) {
      val[offset++] = (char)(rand() % 256);
    }
  } else if (chartype == readable) {
    unsigned isVowel = 0;
    while (length--) {
      val[offset++] =
          (isVowel ? charset[rand() % 5] : charset[5 + rand() % 21]);
      isVowel = !(isVowel);
    }
  } else
    while (length--) {
      unsigned i = rand() % charlen;
      val[offset++] = charset[i];
    }

  free(charset);

  RedisModule_ReplyWithLongLong(ctx, len);
  return REDISMODULE_OK;
}

int testCheckAnd(RedisModuleCtx *ctx) {
  RedisModuleCallReply *r;

  r = RedisModule_Call(ctx, "checkand", "ccccc", "foo", "", "XX", "SET", "bar");
  RMUtil_Assert(RedisModule_CallReplyType(r) == REDISMODULE_REPLY_NULL);
  r = RedisModule_Call(ctx, "checkand", "cccc", "foo", "", "SET", "bar");
  RMUtil_AssertReplyEquals(r,"OK");
  r = RedisModule_Call(ctx, "checkand", "cccc", "foo", "", "SET", "baz");
  RMUtil_Assert(RedisModule_CallReplyType(r) == REDISMODULE_REPLY_NULL);
  r = RedisModule_Call(ctx, "checkand", "cccc", "foo", "bar", "SET", "baz");
  RMUtil_AssertReplyEquals(r,"OK");
  r = RedisModule_Call(ctx, "FLUSHALL", "");
  
  return 0;
}

int testPrepend(RedisModuleCtx *ctx) {
  RedisModuleCallReply *r;

  r = RedisModule_Call(ctx, "set", "cc", "foo", "fghij");
  RMUtil_AssertReplyEquals(r,"OK");
  r = RedisModule_Call(ctx, "prepend", "cc", "foo", "abcde");
  RMUtil_Assert(RedisModule_CallReplyInteger(r) == 10);
  r = RedisModule_Call(ctx, "get", "c", "foo");
  RMUtil_AssertReplyEquals(r,"abcdefghij");
  r = RedisModule_Call(ctx, "FLUSHALL", "");

  return 0;
}

int testSetRangeRand(RedisModuleCtx *ctx) {
  RedisModuleCallReply *r;

  r = RedisModule_Call(ctx, "setrangerand", "ccc", "s", "0", "10");
  RMUtil_Assert(RedisModule_CallReplyInteger(r) == 10);
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

  RMUtil_Test(testCheckAnd);
  RMUtil_Test(testPrepend);
  RMUtil_Test(testSetRangeRand);

  RedisModule_ReplyWithSimpleString(ctx, "PASS");
  return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx) {
  if (RedisModule_Init(ctx, RM_MODULE_NAME, 1, REDISMODULE_APIVER_1) ==
      REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "checkand", CheckAndCommand,
                                "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "prepend", PrependCommand,
                                "write fast deny-oom", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "setrangerand", SetRangeRandCommand,
                                "write fast deny-oom", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "rxstrings.test", TestModule, "write", 0,
                                0, 0) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  return REDISMODULE_OK;
}