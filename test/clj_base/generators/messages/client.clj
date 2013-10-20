(ns clj-base.generators.messages.client
  "Defines test generators for the protobuf messages in ClientProtos.proto"
  (:require [clj-base.messages :as msg]
            [clj-base.generators.messages :refer :all]
            [clj-base.generators.messages.hbase :refer :all]
            [clj-base.generators.messages.filter :as filter]
            [clj-base.generators.messages.cell :as cell]
            [clj-base.generators.messages.comparator :as comp]
            [clojure.data.generators :as gen])
  (:refer-clojure :exclude [get]))

(defn column []
  {:column (gen-bytes)
   :qualifier (gen-bytes)})

(defn get []
  (maybe-assoc {:row (gen-bytes)
                :column (gen/list column)
                :attribute (gen/list name-bytes-pair)
                :max-version (gen/int)
                :cache-blocks (gen/boolean)
                }
               :filter (filter/filter)
               :time-range (time-range)
               :store-limit (gen/int)
               :store-offset (gen/int)))

(defn result []
  (maybe-assoc {:cell (gen/list cell/cell)}
               :associated-cell-count (gen/int)))

(defn get-request []
  (maybe-assoc {:region (region-specifier)
                :get (get)}
               :closest-row-before (gen/boolean)
               :existence-only (gen/boolean)))
(defn get-response []
  (maybe-assoc {}
               :result (result)
               :exists (gen/boolean)))

(defn multi-get-request []
  (maybe-assoc {:region (region-specifier)
                :get (gen/list get)}
               :closest-row-before (gen/boolean)
               :existence-only (gen/boolean)))
(defn multi-get-response []
  {:result (gen/list result)
   :exists (gen/list gen/boolean)})

(defn condition []
  {:row (gen-bytes)
   :family (gen-bytes)
   :qualifier (gen-bytes)
   :compare-type (compare-type)
   :comparator (comp/comparator)})

(defn durability []
  (rand-nth (org.apache.hadoop.hbase.protobuf.generated.ClientProtos$MutationProto$Durability/values)))

(defn mutation-type []
  (rand-nth (org.apache.hadoop.hbase.protobuf.generated.ClientProtos$MutationProto$MutationType/values)))

(defn delete-type []
  (rand-nth (org.apache.hadoop.hbase.protobuf.generated.ClientProtos$MutationProto$DeleteType/values)))

(defn qualifier-value []
  (maybe-assoc {}
               :qualifier (gen-bytes)
               :value (gen-bytes)
               :timestamp (gen/long)
               :delete-type (delete-type)
               :tags (gen-bytes)))

(defn column-value []
  (maybe-assoc {:family (gen-bytes)}
               :qualifier-value (qualifier-value)))

(defn mutation-proto []
  (maybe-assoc {:column-value (gen/list column-value)
                :attribute (gen/list name-bytes-pair)
                :durability (durability)}
               :row (gen-bytes)
               :mutate-type (mutation-type)
               :timestamp (gen/long)))

(defn mutate-request []
  (maybe-assoc {:region (region-specifier)
                :mutation (mutation-proto)}
               :condition (condition)))
(defn mutate-response []
  (maybe-assoc {:result (result)
                :processed (gen/boolean)}))

(defn scan []
  (maybe-assoc {:column (gen/list column)
                :attribute (gen/list name-bytes-pair)
                :max-version (gen/int)
                :cache-blocks (gen/boolean)}
               :start-row (gen-bytes)
               :stop-row (gen-bytes)
               :filter (filter/filter)
               :time-range (time-range)
               :batch-size (gen/int)
               :max-result-size (gen/long)
               :store-limit (gen/int)
               :store-offset (gen/int)
               :load-column-families-on-demand (gen/boolean)
               :small (gen/boolean)))

(defn scan-request []
  (maybe-assoc {}
               :region (region-specifier)
               :scan (scan)
               :scanner-id (gen/long)
               :number-of-rows (gen/int)
               :close-scanner (gen/boolean)
               :next-call-seq (gen/long)))
(defn scan-response []
  (maybe-assoc {:cells-per-result (gen/list gen/int)}
               :scanner-id (gen/long)
               :more-result (gen/boolean)
               :ttl (gen/int)
               :results (result)))

(defn family-path []
  {:family (gen-bytes)
   :path (gen/string)})

(defn bulk-load-hfile-request []
  (maybe-assoc {:region (region-specifier)
                :family-path (gen/list family-path)}
               :assign-seq-num (gen/boolean)))
(defn bulk-load-hfile-response []
  {:loaded (gen/boolean)})

(defn coprocessor-service-call []
  {:row (gen-bytes)
   :service-name (gen/string)
   :method-name (gen/string)
   :request (gen-bytes)})

(defn coprocessor-service-request []
  {:region (region-specifier)
   :call (coprocessor-service-call)})

(defn coprocessor-service-response []
  {:region (region-specifier)
   :call (coprocessor-service-call)})

(defn multi-action []
  (maybe-assoc {}
               :mutation (mutation-proto)
               :get (get)))

(defn action-result []
  (maybe-assoc {}
               :value (result)
               :exception (name-bytes-pair)))

(defn multi-request []
  (maybe-assoc {:region (region-specifier)
                :action (multi-action)}
               :atomic (gen/boolean)))
(defn multi-response []
  {:result (gen/list action-result)})
