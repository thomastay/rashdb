## Tasks

1. Check datatype of field value before inserting it onto disk. Right now we just assume it's serializable (this might be part of the custom encoding scheme thing)
1. Allow writing more than one data val to disk

## Features

1. Implement paging
1. Implement a better VarInt, like the one sqlite's author recommended. Where the first byte tells you how many bytes are in the integer
   e.g.
   0-240 (1 byte): just 0-240 as itself
   241-248 (2 bytes): 240 + 256 \* (X-241) + A1 (max of 2287)
   249 (3 bytes): A1..A2 as big endian integer (2288 - 65535)
   250 (4 bytes): A1..A3 as big-endian integer (2 ** 16 to 2**24-1)
   ...
   255 (9 bytes): A1..A8 as a big endian integer. (2 ** 56 to 2 ** 64-1)
   8 bytes can store vals of length 2^64-1, which is as much as a 64-bit machine can hold anyway.
1. Create an encoding scheme instead of relying on messagepack to do it for you

## Optimizations

1. Use messagepack encoder pool

## DONE

1. Update dump script to dump data elts too (DONE)
1. Write varints for length instead of uint32
