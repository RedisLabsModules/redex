RedisEx: extension modules to Redis' native data types and commands
===

Included modules:
 * [rxkey](#rxkey) - extended key commands
 * [rxstring](#rxstring) - extended String commands
 * [rxhash](#rxhash) - extended Hash commands
 * [rxlist](#rxlist) - extended List commands
 * [rxset](#rxset) - extended Set commands
 * [rxzset](#rxzset) - extended Sorted Set commands
 * [rxgeo](#rxgeo) - extended Geo Set commands

Quick start guide
---

1. Build a Redis server with support for modules.
2. Build the redex modules: `make`
3. To load a module, Start Redis with the `--loadmodule /path/to/module.so` option, add it as a directive to the configuration file or send a `MODULE LOAD` command.

# rxkey

This module provides extended Redis key commands.

## `PKEYS pattern`

Returns keys with names matching `pattern`. `pattern` should be given as a POSIX Extended Regular Expression.

**Return:** Array of Strings, the key names matching. 

## `PDEL pattern`

Deletes keys with names matching `pattern`. `pattern` should be given as a POSIX Extended Regular Expression.

**Return:** Integer, the number of keys deleted. 

# rxstring

This module provides extended Redis String commands.

## `CHECKAND key value [XX] <command> [arg1] [...]`

Checks a String 'key' for 'value' equality and executes a command on it.
Command can be any of the following Redis String commands: `APPEND`, `DECR[BY]`
`GETSET`, `INCR[BY]`, `INCRBYFLOAT`, `PSETEX`, `SET[EX|NX]`.
The `XX` flag means that the key must exist for the equality to be evaluated.

Note: the key shouldn't be repeated for the executed command.

**Reply:** Null if not equal or for non existing key when the `XX` flag is used.
On success, the reply depends on the actual command executed.

## `SETRANGERAND key offset length charset charcase`
Generates a random string, starting at 'offset' and of length 'length'. An
optional `charset` may be provided:

 * `ALPHA` - letters only: a-z
 * `DIGIT` - digits only: 0-9
 * `ALNUM` - letters and digits
 * `PUNC` - all printable characters other than alphanumerics
 * `HEX` - hexadecimal: a-f, 0-9
 * `CURSE` - censored profanity (!@#$%^&?*)
 * `BINARY` - all characters between 0 and 255
 * `READABLE` - letters only, but more pronounceable
 * `TEXT` - this is the default, any printable character (union of `ALPHA` + `DIGIT` + `PUNC`)

Additionally, an optional `charcase` argument can be provided:
 * `MIXEDCASE` - this is the default, a mix of upper and lower case. Treated as `LOWERCASE` charcase for `HEX` and `READABLE` charsets.
 * `LOWERCASE` - uses only lowercase letters
 * `UPPERCASE` - uses only uppercase letters

**Reply:** Integer, the length of the String after it was modified.

# rxhash

This module provides extended Redis Hash commands.

## `HGETSET key field value`

Sets the `field` in Hash `key` to `value` and returns the previous value, if any.

**Reply:** String, the previous value or NULL if `field` didn't exist.

# rxlist

This module provides extended Redis List commands.

## `LPUSHCAPPED key cap ele [ele ...]`

Pushes elements to the head of a list, but trims it from the opposite end to `cap` * afterwards if reached.

**Reply:** Integer, the list's new length.

## `RPUSHCAPPED key cap ele [ele ...]`

Pushes elements to the tail of a list, but trims it from the opposite end to `cap` * afterwards if reached.

**Reply:** Integer, the list's new length.

## `LPOPRPUSH srclist dstlist`

Pops an element from the head of `srclist` and pushes it to the tail of `dstlist`.

**Reply:** Bulk string, the element.

## `LMPOP list count`

Pops `count` elements from the head of `list`.
If less than `count` elements are available, it pops as many as possible.

**Reply:** Array of popped elements.

## `RMPOP list count`

Pops `count` elements from the tail of `list`.
If less than `count` elements are available, it pops as many as possible.

Note: RMPOP returns the elements in head-to-tail order.

**Reply:** Array of popped elements.

## `LSPLICE srclist dstlist count [ATTACH end] [ORDER ASC|DESC|NOEFFORT]`
Moves `count` from one end of `srclist` to one of `dstlist`'s ends. If less than count elements are available, it moves as much elements as possible. A positive count removes elements from the head of `srclist`, and negative from its end.
The optional `ATTACH` subcommand specifies the end of `dstlist` to which elements are added and `end` can be either 0 meaning list's head (the default), or -1 for its tail.
To maintain the order of elements from `srclist`, LSPLICE may perform extra work depending on the `count` sign and `end`.
The optional `ORDER` subscommand specifies how elements will appear in `destlist`. The default `ASC` order means that the series of attached elements will be ordered as in the source list from left to right. `DESC` will cause the elements to be reversed.
`NOEFFORT` avoids the extra work, so the order determined is:

|count | end | `NOEFFORT`
+------+-----+------------
|  +   |  0  | DESC
|  -   |  0  | ASC
|  +   | -1  | ASC
|  -   | -1  | DESC

**Reply:** Integer, the remaining number of elements in 'srclist'.
Adapted from: redis/src/modules/helloworld.c

# rxset

This module provides extended Redis Set commands.

## `MSISMEMBER key1 [key2 ...] member`

Checks for `member`'s membership in multiple sets.

**Reply:** Integer, the count of sets to which `member` belongs.

# rxzset

This module provides extended Redis Sorted Set commands.

## `ZPOP key [WITHSCORE]`

Pops the element with the lowest score from a Sorted Set.

**Reply** Array reply, the element popped, optionally with the score in case that the 'WITHSCORE' option is given.

## `ZREVPOP key [WITHSCORE]`

Pops the element with the highest score from a Sorted Set.

**Reply** Array reply, the element popped, optionally with the score in case that the 'WITHSCORE' option is given.

## `ZADDCAPPED zset cap score member [score member ...]`

Adds members to a Sorted Set, keeping it at `cap` cardinality. Removes top scoring members as needed to meet the limit.

**Reply:** Integer, the number of members added.

## `ZADDREVCAPPED zset cap score member [score member ...]`

Adds members to a Sorted Set, keeping it at `cap` cardinality. Removes bottom scoring members as needed to meet the limit.

**Reply:** Integer, the number of members added.

## `MZRANK key ele [ele ...]`

A variadic variant for `ZRANK`, returns the ranks of multiple members in a Sorted Set.

**Reply:** Array of Integers.

## `MZREVRANK key ele [ele ...]`

A variadic variant for `ZREVRANK`, returns the reverse ranks of multiple members in a Sorted Set.

**Reply:** Array of Integers.

## `MZSCORE key ele [ele ...]`

A variadic variant for `ZSCORE`, returns the scores of multiple members in a Sorted Set.

**Reply:** Array of Strings.

# rxgeo

This module provides extended Redis Geo Set commands.

## `GEOCLUSTER geoset radius unit min-points [namespace]`

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
