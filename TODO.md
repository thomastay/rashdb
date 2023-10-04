## Tasks

1. Check datatype of field value before inserting it onto disk. Right now we just assume it's serializable (this might be part of the custom encoding scheme thing)

## Features

1. Implement paging
1. Allow multiple primary keys
1. Create an encoding scheme instead of relying on messagepack to do it for you

## Optimizations

1. Use messagepack encoder pool

## DONE

1. Update dump script to dump data elts too (DONE)
1. Write varints for length instead of uint32
1. Allow writing more than one data val to disk
1. Implement a better VarInt, like the one sqlite's author recommended. Where the first byte tells you how many bytes are in the integer
