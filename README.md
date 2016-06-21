Database Agnostic Replication
=============================

The main objective of the project is to create database independent
synchronisation tool. The cost of independency will be the limit to database
structure, which should support versions and local (not replicated) values
storage.

The basic algorithm for the synchronisation is going to be the one from
open-source database CouchDB. With high probability DAR will pretend to be
a couchdb node by supporting all relevant API entry points.

The first database supported as backend will be PostgreSQL because of its
native support of JSON type. The second backend storage will a mobile
storage like Sqlite or Forestdb by Couchbase. Thus there will be a tool
to connect a mobile application directly to an asynchronous storage taking out
your web-application layer. Certainly the mobile database support suppose
the implementation on a mobile platform.

Further plans are to add support for MySQL and some big data NoSQL storages.

The Roadmap
-----------

1. ~~Create a versioned in-memory storage to test replication algorithm without dealing with a database server setup.~~
2. ~~Implement and properly test replication.~~
3. Try / adapt the code to work with a key-value persistent storage (start with SQlite).
4. Wrap the algorithm to server environment.
5. Deal with a proper receipt of PostgreSQL setup and database structure, which would be efficient enough to work in backend.
6. Add authentication support.
7. Start mobile version implementation. (Rewrite on C++ to build from the same code on Linux / iOS / Android?)
