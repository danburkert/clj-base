(ns clj-base.generators.messages.filter
  "Defines test generators for the protobuf messages in FilterProtos.clj"
  (:require [clj-base.messages :as msg]
            [clj-base.generators.messages :refer :all]
            [clj-base.generators.messages.hbase :as hbase]
            [clj-base.generators.messages.comparator :as comp]
            [clojure.data.generators :as gen])
  (:refer-clojure :exclude [filter]))

(defn filter []
  (maybe-assoc {:name (gen/string)}
               :serialized-filter (gen-bytes)))

(defn column-count-get-filter []
  {:limit (gen/int)})

(defn column-pagination-filter []
  (maybe-assoc {:limit (gen/int)}
               :offset (gen/int)
               :column-offset (gen-bytes)))

(defn column-prefix-filter []
  {:prefix (gen-bytes)})

(defn column-range-filter []
  (maybe-assoc {}
               :min-column (gen-bytes)
               :min-column-inclusive (gen/boolean)
               :max-column (gen-bytes)
               :max-column-inclusive (gen/boolean)))

(defn compare-filter []
  (maybe-assoc {:compare-op (compare-type)}
               :comparator (comp/comparator)))

(defn dependent-column-filter []
  (maybe-assoc {:compare-filter (compare-filter)}
               :column-family (gen-bytes)
               :column-qualifier (gen-bytes)
               :drop-dependent-column (gen/boolean)))

(defn family-filter []
  {:compare-filter (compare-filter)})

(defn filter-list []
  {:operator (rand-nth (org.apache.hadoop.hbase.protobuf.generated.FilterProtos$FilterList$Operator/values))
   :filters (gen/list filter)})

(defn filter-wrapper []
  (:filter (filter)))

(defn first-key-only-filter [] {})

(defn first-key-value-matching-qualifiers-filter []
  {:qualifiers (gen/list gen-bytes)})

(defn fuzzy-row-filter []
  {:fuzzy-keys-data (gen/list bytes-bytes-pair)})

(defn inclusive-stop-filter []
  (maybe-assoc {}
               :stop-row-key (gen-bytes)))

(defn key-only-filter []
  {:len-as-val (gen/boolean)})

(defn multiple-column-prefix-filter []
  {:sorted-prefixes (gen/list gen-bytes)})

(defn page-filter []
  {:page-size (gen/long)})

(defn prefix-filter []
  {:prefix (gen-bytes)})

(defn qualifier-filter []
  {:compare-filter (compare-filter)})

(defn random-row-filter []
  {:chance (gen/float)})

(defn row-filter []
  {:compare-filter (compare-filter)})

(defn single-column-value-filter []
  (maybe-assoc {:compare-op (compare-type)
                :comparator (comp/comparator)}
               :column_family (gen-bytes)
               :column_qualifier (gen-bytes)
               :filter-if-missing (gen/boolean)
               :latest-version-only (gen/boolean)))

(defn single-column-value-exclude-filter []
  {:single-column-value-filter (single-column-value-filter)})

(defn skip-filter []
  (:filter (filter)))

(defn timestamp-filter []
  {:timestamps (gen/list gen/long)})

(defn value-filter []
  {:compare-filter (compare-filter)})

(defn while-match-filter []
  (:filter (filter)))
