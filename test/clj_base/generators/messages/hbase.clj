(ns clj-base.generators.messages.hbase
  "Defines test generators for the protobuf messages in HBaseProtos.proto"
  (:require [clj-base.messages :as msg]
            [clj-base.generators.messages :refer :all]
            [clojure.data.generators :as gen]))

(defn coprocessor []
  {:name (gen/string)})

(defn name-string-pair []
  {:name (gen/string)
   :value (gen/string)})

(defn name-bytes-pair []
  (maybe-assoc {:name (gen/string)}
               :value (gen-bytes)))

(defn bytes-bytes-pair []
  {:first (gen-bytes)
   :second (gen-bytes)})

(defn name-int-64-pair []
  (maybe-assoc {}
               :name (gen/string)
               :value (gen/long)))

(defn table-name []
  {:namespace (hbase-identifier)
   :qualifier (hbase-identifier)})

(defn column-family-schema []
  {:name (gen-bytes)
   :attributes (bytes-bytes-pair)
   :configuration (name-string-pair)})

(defn table-schema []
  (maybe-assoc {:attributes (gen/list bytes-bytes-pair)
                :column-families (gen/list column-family-schema)
                :configuration (name-string-pair)}
               :table-name (gen/string)))

(defn region-info []
  (maybe-assoc {:region-id (gen/long)
                :table-name (table-name)}
               :start-key (gen-bytes)
               :end-key (gen-bytes)
               :offline (gen/boolean)
               :split (gen/boolean)))

(defn server-name []
  (maybe-assoc {:host-name (gen/string)}
               :port (gen/int)
               :start-code (gen/long)))

(defn favored-nodes []
  {:favored-node (gen/list server-name)})

(defn region-specifier []
  {:type (rand-nth (org.apache.hadoop.hbase.protobuf.generated.HBaseProtos$RegionSpecifier$RegionSpecifierType/values))
   :value (gen-bytes)})

(defn time-range []
  (maybe-assoc {}
               :from (gen/long)
               :to (gen/long)))

(defn compare-type []
  (rand-nth (org.apache.hadoop.hbase.protobuf.generated.HBaseProtos$CompareType/values)))

(defn snapshot-description []
  (maybe-assoc {:name (gen/string)
                :creation-time 0}
               :table (gen/string)
               :creation-time  (gen/long)
               :type (rand-nth (org.apache.hadoop.hbase.protobuf.generated.HBaseProtos$SnapshotDescription$Type/values))
               :version (gen/int)))

(defn empty-msg [] {})

(defn long-msg [] {:long-msg (gen/long)})

(defn big-decimal-msg [] {:bigdecimal-msg (gen-bytes)})

(defn uuid [] {:least-sig-bits (gen/long)
               :most-sig-bits (gen/long)})

(defn namespace-descriptor []
  {:name (gen-bytes)
   :configuration (gen/list name-string-pair)})
