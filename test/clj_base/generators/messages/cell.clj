(ns clj-base.generators.messages.cell
  "Defines test generators for the protobuf messages in CellProtos.clj"
  (:require [clj-base.messages :as msg]
            [clj-base.generators.messages :refer :all]
            [clojure.data.generators :as gen]))

(defn cell-type []
  (rand-nth (org.apache.hadoop.hbase.protobuf.generated.CellProtos$CellType/values)))

(defn cell []
  (maybe-assoc {}
               :row (gen-bytes)
               :family (gen-bytes)
               :qualifier (gen-bytes)
               :timestamp (gen-bytes)
               :cell-type (gen-bytes)
               :value (gen-bytes)
               :tags (gen-bytes)))

(defn key-value []
  (maybe-assoc {:row (gen-bytes)
                :family (gen-bytes)
                :qualifier (gen-bytes)}
               :timestamp (gen-bytes)
               :cell-type (gen-bytes)
               :value (gen-bytes)
               :tags (gen-bytes)))
