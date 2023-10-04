# Rashdb - you'd be rash to use it

Rash DB is a learning project to build a toy application database that is similar to sqlite and boltdb.
I am also WIP writing a blog post about building rashdb. Stay tuned!

## Goals

1. Completely client side database (aka an application database), much like sqlite, boltdb, duckdb
1. Document everything, this is a learning project
1. Supports MVCC where readers aren't blocking writers
1. Full ACID semantics
1. Single writer for simplicity in design
1. Don't store freelist in an array
1. Support B-trees as main page type, and LSM trees as an optimized variant of the page type
