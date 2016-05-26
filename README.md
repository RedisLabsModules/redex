RedisEx: extension modules to Redis' native data types and commands
===

Exactly what the doctor ordered.

Included modules:
 * [rxkeys](#rxkeys) - extended keys commands ([Module Hub page](http://redismodules.com/modules/rxkeys/))
 * [rxstrings](#rxstrings) - extended Strings commands ([Module Hub page](http://redismodules.com/modules/rxstrings/))
 * [rxhashes](#rxhashes) - extended Hashes commands ([Module Hub page](http://redismodules.com/modules/rxhashes/))
 * [rxlists](#rxlists) - extended Lists commands ([Module Hub page](http://redismodules.com/modules/rxlists/))
 * [rxsets](#rxsets) - extended Sets commands ([Module Hub page](http://redismodules.com/modules/rxsets/))
 * [rxzsets](#rxzsets) - extended Sorted Sets commands ([Module Hub page](http://redismodules.com/modules/rxzsets/))
 * [rxgeo](#rxgeo) - extended Geo Sets commands ([Module Hub page](http://redismodules.com/modules/rxgeo/))

Quick start guide
---

1. Build a Redis server with support for modules (currently available from the [unstable branch](https://github.com/antirez/redis/tree/unstable)).
2. Build the redex modules: `make`
3. To load a module, Start Redis with the `--loadmodule /path/to/module.so` option, add it as a directive to the configuration file or send a `MODULE LOAD` command.

# rxkeys

This module provides extended Redis keys commands.

## `PKEYS pattern`

> Time complexity: O(N) where N is the number of keys in the database.

Returns keys with names matching `pattern`. `pattern` should be given as a POSIX Extended Regular Expression.

**Return:** Array of Strings, the key names matching. 

## `PDEL pattern`

> Time complexity: O(N)+O(M) where N is the number of keys in the database and M is the number of elements to delete. The deletion's complexity is O(1) for Strings, and O(L) for keys with multiple elements, where L is the number of elements.

Deletes keys with names matching `pattern`. `pattern` should be given as a POSIX Extended Regular Expression.

**Return:** Integer, the number of keys deleted. 

# rxstrings

This module provides extended Redis Strings commands.

## `CHECKAND key value [XX] <command> [arg1] [...]`

> Time complexity: O(1) + O(`command`)

Checks a String 'key' for 'value' equality and executes a Redis `command` on it.

The `command` can be any of the following Redis String commands:

* `APPEND`
* `DECR[BY]`
* `GETSET`
* `INCR[BY]`
* `INCRBYFLOAT`
* `PSETEX`
* `SET[EX|NX]`

The `XX` flag means that the key must exist for the equality to be evaluated.

Note: the key shouldn't be repeated for the executed command.

**Reply:** Null if not equal or for non existing key when the `XX` flag is used.
On success, the reply depends on the actual command executed.

## `PREPEND key value`

> Time complexity: O(1). The amortized time complexity is O(1) assuming the prepended value is small and the already present value is of any size, since the dynamic string library used by Redis will double the free space available on every reallocation.

Prepends a value to a String key.

If `key` does not exist it is created and set as an empty string, so `PREPEND` will be similar to [`SET`](http://redis.io/commands/set) in this special case.

**Reply:** Integer, the length of the string after the prepend operation.

## `SETRANGERAND key offset length charset charcase`

> Time complexity: O(N) where N is the size of the range generated.

Generates a random string, starting at 'offset' and of length 'length'. An
optional `charset` may be provided:

 * `ALPHA` - letters only: a-z
 * `DIGIT` - digits only: 0-9
 * `ALNUM` - letters and digits
 * `PUNC` - all printable characters other than alphanumerics
 * `HEX` - hexadecimal: a-f, 0-9
 * `CURSE` - censored profanity (!@#$%^&?\*)
 * `BINARY` - all characters between 0 and 255
 * `READABLE` - letters only, but more pronounceable
 * `TEXT` - this is the default, any printable character (union of `ALPHA` + `DIGIT` + `PUNC`)

Additionally, an optional `charcase` argument can be provided:
 * `MIXEDCASE` - this is the default, a mix of upper and lower case. Treated as `LOWERCASE` charcase for `HEX` and `READABLE` charsets.
 * `LOWERCASE` - uses only lowercase letters
 * `UPPERCASE` - uses only uppercase letters

Credit: Meni Katz

**Reply:** Integer, the length of the String after it was modified.

# rxhashes

This module provides extended Redis Hashes commands.

## `HGETSET key field value`

> Time complexity: O(1)

Sets the `field` in Hash `key` to `value` and returns the previous value, if any.

**Reply:** String, the previous value or NULL if `field` didn't exist.

# rxlists

This module provides extended Redis Lists commands.

## `LPUSHCAPPED key cap ele [ele ...]`

> Time complexity: O(N+M) where N is the number of elements added and M is the number of elements trimmed.

Pushes elements to the head of a list, but trims it from the opposite end to `cap` afterwards, if reached.

**Reply:** Integer, the list's new length.

## `RPUSHCAPPED key cap ele [ele ...]`

> Time complexity: O(N+M) where N is the number of elements added and M is the number of elements trimmed.

Pushes elements to the tail of a list, but trims it from the opposite end to `cap` afterwards, if reached.

**Reply:** Integer, the list's new length.

## `LPOPRPUSH srclist dstlist`

> Time complexity: O(1)

Pops an element from the head of `srclist` and pushes it to the tail of `dstlist`.

**Reply:** Bulk string, the element.

## `LMPOP list count`

> Time complexity: O(N) where N is the number of elements that were popped.

Pops `count` elements from the head of `list`.
If less than `count` elements are available, it pops as many as possible.

**Reply:** Array of popped elements.

## `RMPOP list count`

> Time complexity: O(N) where N is the number of elements that were popped.

Pops `count` elements from the tail of `list`.
If less than `count` elements are available, it pops as many as possible.

Note: RMPOP returns the elements in head-to-tail order.

**Reply:** Array of popped elements.

## `LSPLICE srclist dstlist count`

> Time complexity: O(N) where N is the number of elements moved.

Moves 'count' elements from the tail of 'srclist' to the head of 'dstlist'.

If less than count elements are available, it moves as much elements as possible.
 
**Reply:** Integer, the new length of srclist.

Copied from: redis/src/modules/helloworld.c

## `LXSPLICE srclist dstlist count [ATTACH end] [ORDER ASC|DESC|NOEFFORT]`

> Time complexity: O(N) where N is the number of elements moved.

Moves `count` from one end of `srclist` to one of `dstlist`'s ends.

If less than count elements are available, it moves as much elements as possible.

A positive count removes elements from the head of `srclist`, and negative from its end.

The optional `ATTACH` subcommand specifies the end of `dstlist` to which elements are added and `end` can be either 0 meaning list's head (the default), or -1 for its tail.

To maintain the order of elements from `srclist`, `LSPLICE` may perform extra work depending on the `count` sign and `end`.

The optional `ORDER` subscommand specifies how elements will appear in `destlist`. The default `ASC` order means that the series of attached elements will be ordered as in the source list from left to right. `DESC` will cause the elements to be reversed.
`NOEFFORT` avoids the extra work, so the order determined is:

|count | end | `NOEFFORT`
|------|-----|-----------
|  +   |  0  | DESC
|  -   |  0  | ASC
|  +   | -1  | ASC
|  -   | -1  | DESC

**Reply:** Integer, the remaining number of elements in 'srclist'.

# rxsets

This module provides extended Redis Sets commands.

## `MSISMEMBER key1 [key2 ...] member`

> Time complexity: O(N) where N is the number of keys.

Checks for `member`'s membership in multiple sets.

**Reply:** Integer, the count of sets to which `member` belongs.

# rxzsets

This module provides extended Redis Sorted Sets commands.

## `ZPOP key [WITHSCORE]`

> Time complexity: O(LogN) where N is the number of elements in the Sorted Set.

Pops the element with the lowest score from a Sorted Set.

**Reply** Array reply, the element popped, optionally with the score in case that the 'WITHSCORE' option is given.

## `ZREVPOP key [WITHSCORE]`

> Time complexity: O(LogN) where N is the number of elements in the Sorted Set.

Pops the element with the highest score from a Sorted Set.

**Reply** Array reply, the element popped, optionally with the score in case that the 'WITHSCORE' option is given.

## `ZADDCAPPED zset cap score member [score member ...]`

> Time complexity: O(N\*LogM) where N is the number of elements added and M is the number of elements in the Sorted Set.

Adds members to a Sorted Set, keeping it at `cap` cardinality. Removes top scoring members as needed to meet the limit.

**Reply:** Integer, the number of members added.

## `ZADDREVCAPPED zset cap score member [score member ...]`

> Time complexity: O(N\*LogM) where N is the number of elements added and M is the number of elements in the Sorted Set.

Adds members to a Sorted Set, keeping it at `cap` cardinality. Removes bottom scoring members as needed to meet the limit.

**Reply:** Integer, the number of members added.

## `MZRANK key ele [ele ...]`

> Time complexity: O(N\*LogM) where N is the number of elements passed as arguments to the command and M is the number of elements in the Sorted Set.

A variadic variant for `ZRANK`, returns the ranks of multiple members in a Sorted Set.

**Reply:** Array of Integers.

## `MZREVRANK key ele [ele ...]`

> Time complexity: O(N\*LogM) where N is the number of elements passed as arguments to the command and M is the number of elements in the Sorted Set.

A variadic variant for `ZREVRANK`, returns the reverse ranks of multiple members in a Sorted Set.

**Reply:** Array of Integers.

## `MZSCORE key ele [ele ...]`

> Time complexity: O(N\*LogM) where N is the number of elements passed as arguments to the command and M is the number of elements in the Sorted Set.

A variadic variant for `ZSCORE`, returns the scores of multiple members in a Sorted Set.

**Reply:** Array of Strings.

## `ZUNIONTOP K numkeys key [key ...] [WEIGHTS weight [weight ...]] [WITHSCORES]`

> Time complexity: O(numkeys\*log(N) + K\*log(numkeys)) where N is the number of elements in a Sorted Set.

Union multiple Sorted Sets and return the `K` elements with lowest scores. Refer to [`ZUNIONSTORE`](http://redis.io/commands/zunionstore)'s documentation for details on using the command.

**Reply:** Array reply, the top k elements (optionally with the score, in case the 'WITHSCORES' option is given).

## `ZUNIONREVTOP K numkeys key [key ...] [WEIGHTS weight [weight ...]] [WITHSCORES]`

> Time complexity: O(numkeys\*log(N) + K\*log(numkeys)) where N is the number of elements in a Sorted Set.

Union multiple Sorted Sets and return the `K` elements with highest scores. Refer to [`ZUNIONSTORE`](http://redis.io/commands/zunionstore)'s documentation for details on using the command.

**Reply:** Array reply, the top k elements (optionally with the score, in case the 'WITHSCORES' option is given).

# rxgeo

This module provides extended Redis Geo Sets commands.

## `GEOCLUSTER geoset radius unit min-points [namespace]`

> Time complexity: O(N\*LogN) where N is the number of points in the Geo Set. 

Density based spatial clustering with random sampling. Creates a set of Geo Sets from `geoset`, each being a cluster.
`radius` is the maximum distance between near neighbours in the cluster, and `min-points` is a cluster's minimum size. 
The default cluster `namespace` is 'DBRS', geo cluster keys are created using the following syntax: `namespace:{geoset}:...`. 

Note: the sampling isn't really random, but that shouldn't matter.

More information:

 * http://www.ucalgary.ca/wangx/files/wangx/dbrs.pdf
 * http://www.ucalgary.ca/wangx/files/wangx/comparison.pdf

**Reply:** Integer, the number of clusters created

# Misc

TODO
---

Oh so much :)

Contributing
---

Issue reports, pull and feature requests are welcome.

License
---

AGPLv3 - see [LICENSE](LICENSE)
