from couchbase import Collection, GetResult, KeyNotFoundException, Durability, KeyExistsException, PersistTo, \
    ReplicateTo, TimeoutException
import couchbase.exceptions
from couchbase.collection import Collection
from couchbase.cluster import Cluster
import logging
collection=Cluster().bucket("fred").default_collection()

#tag::handle_retryable[]
# This an example of error handling for idempotent operations (such as the full-doc op seen here).

def change_email(collection,  # type: Collection
                 maxRetries  # type: int
                 ):
    try:
        result = collection.get("doc_id")  # type: GetResult

        if not result:
            raise couchbase.exceptions.KeyNotFoundException()
        else:
            content = result.content

        content["email"]="john.smith@couchbase.com"

        collection.replace("doc_id", content);
    except couchbase.exceptions.CouchbaseError as err:
        # isRetryable will be true for transient errors, such as a CAS mismatch (indicating
        # another agent concurrently modified the document), or a temporary failure (indicating
        # the server is temporarily unavailable or overloaded).  The operation may or may not
        # have been written, but since it is idempotent we can simply retry it.
        if err.is_retryable:
            if maxRetries > 0:
                logging.info("Retrying operation on retryable err " + err)
            change_email(collection, maxRetries - 1);
        else:
            # Errors can be transient but still exceed our SLA.
            # If the err is not isRetryable, there is perhaps a more permanent or serious error,
            # such as a network failure.
            logging.error("Too many attempts, aborting on err " + err)
            raise


MAX_RETRIES=5
try:
    change_email(collection, MAX_RETRIES)
except RuntimeError as err:
    # What to do here is highly application dependent.  Options could include:
    # - Returning a "please try again later" error back to the end-user (if any)
    # - Logging it for manual human review, and possible follow-up with the end-user (if any)
    logging.error("Failed to change email")
#end::handle_retryable[]

#tag::KeyNotFoundException[]

try:
    collection.replace("my-key", {})
except KeyNotFoundException:
    # key does not exist
    pass
#end::KeyNotFoundException[]

#tag::KeyExistsException[]
try:
    collection.insert("my-key",{})
except KeyExistsException:
    # key already exists
    pass
#end::KeyExistsException[]

#tag::CASMismatchException[]
try:
    result = collection.get("my-key")
    collection.replace("my-key", {}, cas = result.cas)
except couchbase.exceptions.CASMismatchException:
    # the CAS value has changed
    pass
#end::CASMismatchException[]

#tag::DurabilitySyncWriteAmbiguousException[]
try:
    collection.upsert("my-key", {}, durability_level=Durability.PERSIST_TO_MAJORITY)
except couchbase.exceptions.DurabilitySyncWriteAmbiguousException:
    # durable write request has not completed, it is unknown whether the request met the durability requirements or not
    pass
#end::DurabilitySyncWriteAmbiguousException[]

#tag::DurabilityInvalidLevelException[]
try:
    collection.upsert("my-key", {}, durability_level=Durability.PERSIST_TO_MAJORITY)
except couchbase.exceptions.DurabilityInvalidLevelException:
    # cluster not able to meet durability requirements
    pass
#end::DurabilityInvalidLevelException[]

#tag::ReplicaNotConfiguredException[]
try:
    collection.upsert("my-key", {}, persist_to=PersistTo.FOUR, replicate_to=ReplicateTo.THREE)
except couchbase.exceptions.ReplicaNotConfiguredException:
    # cluster doesn't have replicas configured
    pass
#end::ReplicaNotConfiguredException[]

#tag::DurabilityImpossibleException[]
try:
    collection.upsert("my-key", {}, persist_to=PersistTo.FOUR, replicate_to=ReplicateTo.THREE)
except couchbase.exceptions.DurabilityImpossibleException:
    # cluster not able to meet durability requirements
    pass
#end::DurabilityImpossibleException[]

#tag::TimeoutException[]
try:
    collection.upsert("my-key", {}, persist_to=PersistTo.FOUR, replicate_to=ReplicateTo.THREE)
except TimeoutException:
    # document may or may not have persisted to specified durability requirements
    pass
#end::TimeoutException[]
