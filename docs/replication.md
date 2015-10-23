by Jens Alfke

h2. Introduction

Couchbase Lite's replication protocol is compatible with Apache CouchDB. This interoperability is an important feature, but implementing it was challenging because much of CouchDB's replication protocol is undocumented. In the future I would like to see an explicit spec for replication, to ensure that different products remain compatible. For now I'll document it here, *as I understand it*.

bq. Note: If you want to follow along, this algorithm is implemented in Couchbase Lite's @CBLReplicator@ and its subclasses @CBLPusher@ and @CBLPuller@ (plus a number of helper classes.)

These notes were derived from reading the "API documentation":http://wiki.apache.org/couchdb/Complete_HTTP_API_Reference on the CouchDB wiki and from conversation with engineers who've worked on CouchDB's replicator (Damien Katz and Filipe Manana). But don't take them as gospel.

h2. Protocol? What Protocol?

There really isn't a separate "protocol" per se for replication. Instead, replication uses CouchDB's REST API and data model. It's therefore a bit difficult to talk about replication independently of the rest of CouchDB. In this document I'll focus on the algorithm used, and link to documentation of the APIs it invokes. The "protocol" is simply the set of those APIs operating over HTTP.

h2. Algorithm

h3. Goal

Given a _source_ and a _target_ database, identify all current revisions (including deletions) in the source that do not exist in the target, and copy them (with contents, attachments and histories) to the target. Afterwards, all current revisions in the source exist at the target and have the same revision histories there.

*Secondary goal:* Do this without redundantly transferring the contents of any revisions that already exist at the target.

bq. Note: A _current_ revision is one that has not been replaced, i.e. a leaf node in the revision tree. Most of the time a document has only one current revision, but multiple current revisions can exist and that's called a _conflict_.

h3. Steps

# Get unique identifiers for the source and target databases (which may just be their URLs, if no UUIDs are available).
# Generate a unique identifier for this replication based on the database IDs, and the filter name and parameters (if any). For instance, you can concatenate these with an unambiguous delimiter and then run that string through a cryptographic digest algorithm like SHA-1. The exact mechanism doesn't matter, because this identifier is used only by a particular implementation.
# Use this identifier to generate the doc ID of a special (_local, non-replicated) document, and get this document from both the source and the target database. The document contains the _last source sequence ID_ (also called a "checkpoint") that was read and processed by the previous replication. If the document is missing in either database, or if its contents are inconsistent, that's OK: the replication will just start from scratch without a checkpoint.
# Fetch the source database's "@_changes@":http://wiki.apache.org/couchdb/HTTP_database_API#Changes feed, starting just past the last source sequence ID (if any). Use the "@?style=all_docs@" URL parameter so that conflicting revisions will be included. In continuous replication you should use the "@?feed=longpoll@" or "@?feed=continuous@" mode and leave the algorithm running indefinitely to process changes as they occur. Filtered replication will specify the name of a filter function in this URL request.
# Collect a group of document/revision ID pairs from the @_changes@ feed and send them to the target database's "@_revs_diff@":http://wiki.apache.org/couchdb/HttpPostRevsDiff. The result will contain the subset of those revisions that are _not_ in the target database.
# @GET@ each such revision from the source database. Use the "@?revs=true@":http://wiki.apache.org/couchdb/HTTP_Document_API#Special_Fields URL parameter to include its list of parent revisions, so the source database can update its revision tree. Use "@?attachments=true@" so the revision data will include attachment bodies. Also use the "@?atts_since@" query parameter to pass a list of revisions that the target already has, so the source can optimize by not including the bodies of attachments already known to the target.
# Collect a group of revisions fetched by the previous step, and store them into the target database using the "@_bulk_docs@":http://wiki.apache.org/couchdb/HTTP_Bulk_Document_API API, with the "@new_edits:false@":http://wiki.apache.org/couchdb/HTTP_Bulk_Document_API#Posting_Existing_Revisions JSON property to preserve their revision IDs.
# After a group of revisions is stored, save a checkpoint: update the _last source sequence ID_ value in the target database. It should be the latest sequence ID for which its revision and all prior to it have been added to the target. (Even if some revisions are rejected by a target validation handler, they still count as 'added' for this purpose.)

There's also a "ladder diagram":http://cl.ly/image/1v013o210345 which shows these steps along with the interaction between the replicator and source/target db's.

h3. Notes

— The replication algorithm does not have to run on either the source's or target's server. It could be run from anywhere with read access to the source and write access to the target. However, it's nearly always run by either the source or target server (and Couchbase Lite only supports those modes). Replication run by the source is commonly called a "push", and by the target is called a "pull". An implementation run by the source or target server may optimize by using lower-level APIs to operate on the local database; for example, it might listen for internal change notifications rather than reading the @_changes@ feed.

— Replication does _not_ transfer obsolete revisions of documents, only the current ones. This derives from the behavior of the @_changes@ feed, which only lists current revisions. Replication does transfer the revision _history_ of each document, which is just the list of IDs of prior revisions; this is to make it possible for the database to identify common ancestors and merge revision histories into a tree.

— Sequence IDs are usually but _not necessarily_ numeric. (Currently the only exception I know of is BigCouch.) Non-numeric sequence IDs are not intrinsically ordered, i.e. they are opaque strings that can only be compared for equality. To compare their ordering (when checkpointing) you have to keep an ordered list of sequence IDs as they appeared in the @_changes@ feed and compare their indices in that.

h3. Performance

— For efficiency, the algorithm should run in parallel, as a data-flow system, with multiple steps active at the same time. This reduces the overhead of network and database latency.

— Also for efficiency, the number of revisions passed in a single @_revs_diff@ or @_bulk_docs@ call should be large. This means the implementation should group together revisions arriving from previous steps until a sufficient number have arrived or sufficient time has elapsed.

— From my limited testing, the performance bottleneck in the current algorithm seems to be in fetching the new revisions from the source. I think this is due to the overhead of handling many separate HTTP requests. In the Couchbase Sync Gateway I've sped up replication by introducing a new nonstandard API call, @_bulk_get@, that fetches revisions in bulk as nested MIME multipart bodies. (The standard @_all_docs@ call can fetch a list of revisions, but currently can't be told to include revision histories.)

- A limited case of the above-mentioned bulk-get optimization is possible with the standard API: revisions of generation 1 (revision ID starts with "@1-@") can be fetched in bulk via @_all_docs@, because by definition they have no revision histories. Unfortunately @_all_docs@ can't include attachment bodies, so if it returns a document whose JSON indicates it has attachments, those will have to be fetched separately. Nonetheless, this optimization can help significantly, and is currently implemented in Couchbase Lite.

h2. API Calls Used

These are the CouchDB REST API calls that Couchbase Lite makes to the remote database.

* @GET /@_db_ @/_local/@_checkpointid_ — To read the last checkpoint
* @PUT /@_db_ @/_local/@_checkpointid_ — To save a new checkpoint

h3. Push Only:

* @PUT /@_db_ — If told to create remote database
* @POST /@_db_ @/_revs_diff@ — To find which revs are not known to the remote db
* @POST /@_db_ @/_bulk_docs@ — To upload revisions
* @POST /@_db_ @/@_docid_ @?new_edits=false@ — To upload a single doc with attachments

h3. Pull Only:

* @GET /@_db_ @/_changes?style=all_docs&feed=@_feed_ @&since=@_since_ @&limit=@_limit_ @&heartbeat=@_heartbeat_ — To find changes since the last pull (_feed_ will be @normal@ or @longpoll@)
* @GET /@_db_ @/@_docid_ @?rev=@_revid_ @&revs=true&attachments=true&atts_since=@_lastrev_ — To download a single doc with attachments
* @POST /@_db_ @/_all_docs?include_docs=true@ — To download first-generation revisions in bulk
