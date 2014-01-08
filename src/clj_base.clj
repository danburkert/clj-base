(ns clj-base
  (:require [byte-streams :refer [to-byte-array]])
  (:refer-clojure :exclude [seq])
  (:import
    [org.hbase.async HBaseClient Scanner]
    [com.stumbleupon.async Deferred]))

(defprotocol Instance
  "Defines operations on an HBase cluster instance."
  (table [this name]
         "Creates a view of the table.  Throws a
          TableNotFoundException if table is not available in the
          HBase instance."))

(defprotocol Table
  "Defines view operations on an HBase table."
  (row-prefix [this prefix]
              "Returns a view of this table restricted to rows whose keys match
               the prefix.")
  (columns [this columns]
           "Returns a view of this table restricted to the set of specified
            columns. A column can be specified as a string, in which case it
            is assumed to be a column family, or as a tuple of
            [family qualifier] or [family [qualifiers]].")
  (versions [this n]
            "Returns a view of this table over the n most-recent versions of
             each cell.")
  (since [this ts]
         "Returns a view of this table since the timestamp, inclusive.")
  (until [this ts]
         "Returns a view of this table until the timestamp, exclusive.")
  )

(deftype HBaseScan [^Scanner scan]
  clojure.lang.Seqable
  (seq [this] this)
  clojure.lang.ISeq
  (first [this]

    )
  )

(deftype HBaseTable [client name filters]
  clojure.lang.Seqable
  (seq [this]

    )

  TableView
  (row-prefix [this prefix])
  (columns [this columns])
  (versions [this n])
  (since [this ts])
  (until [this ts])

  )

(defrecord HBaseInstance [client]
  Instance
  (table [this name]
    (.join (.ensureTableExists client name))
    (->HBaseTable name client {})))

(defn hbase-instance
  "Takes a zookeeper quorum as a sequence of members, or as a string containing
   comma-separated members. A member is a string containing the hostname and
   optionally a colon-separated port.  Throws a TimeoutException if unable to
   connect to the cluster."
  [zk-quorum]
  (let [client (if (string? zk-quorum)
                 (HBaseClient. zk-quorum)
                 (HBaseClient. (clojure.string/join "," zk-quorum)))]
    (.join (.ensureTableExists client ".META.") 1000)
    (->HBaseInstance client)))

(comment
  (def instance (hbase-instance "localhost"))
  (table instance "my-table")

  (def client
    (HBaseClient. "localhost:2181"))
  (identity client)

  (def exists
    (.ensureTableExists client ".META."))
  (identity exists)

  )
