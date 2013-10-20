(ns clj-base.generators.messages.wal
  "Defines test generators for the protobuf messages in WAL.proto"
  (:require [clj-base.messages :as msg]
            [clj-base.generators.messages :refer :all]
            [clj-base.generators.messages.hbase :refer :all]
            [clojure.data.generators :as gen]))

(defn wal-header []
  (maybe-assoc {}
               :has-compression (gen/boolean)))

(defn custom-entry-type []
  (rand-nth (org.apache.hadoop.hbase.protobuf.generated.WALProtos$ScopeType/values))
  )

(defn wal-key []
  (maybe-assoc {:encoded-region-name (gen-bytes)
                :table-name (gen-bytes)
                :log-sequence-number (gen/long)
                :write-time (gen/long)
                :scopes (gen/list family-scope)
                :cluster-ids (gen/list uuid)}
               :following-kv-count (gen/int)
               :custom-entry-type (custom-entry-type)))
