(ns clj-base.generators.messages.admin
  "Defines test generators for the protobuf messages in AdminProtos.proto"
  (:require [clj-base.messages :as msg]
            [clj-base.generators.messages :refer :all]
            [clj-base.generators.messages.hbase :refer :all]
            [clojure.data.generators :as gen]))

(defn get-region-info-request []
  (maybe-assoc {:region (region-specifier)}
               :compaction-state (gen/boolean)))
(defn get-region-info-response []
  (maybe-assoc {:region-info (region-info)}
               :compaction-state (rand-nth (org.apache.hadoop.hbase.protobuf.generated.AdminProtos$GetRegionInfoResponse$CompactionState/values))
               :is-recovering (gen/boolean)))

(defn get-store-file-request []
  {:region (region-specifier)
   :family (gen/list gen-bytes)})
(defn get-store-file-response []
  {:store-file (gen/string)})

(defn get-online-region-request [] {})
(defn get-online-region-response []
  {:region-info (gen/list region-info)})

(defn open-region-request []
  (letfn [(region-open-info []
            {:region (region-info)
             :version-of-offline-node (gen/int)
             :favored-nodes (server-name)})]
    {:open-info (gen/list region-open-info)}))
(defn open-region-response []
  {:opening-state (rand-nth (org.apache.hadoop.hbase.protobuf.generated.AdminProtos$OpenRegionResponse$RegionOpeningState/values))})

(defn close-region-request []
  (maybe-assoc {:region (region-specifier)
                :transition-in-ZK true}
               :version-of-closing-node (gen/int)
               :transition-in-ZK (gen/boolean)
               :destination-server (server-name)))
(defn close-region-response []
  {:closed (gen/boolean)})

(defn flush-region-request []
  (maybe-assoc {:region (region-specifier)}
               :if-older-than-ts (gen/long)))
(defn flush-region-response []
  (maybe-assoc {:last-flush-time (gen/long)}
               :flushed (gen/boolean)))

(defn split-region-request []
  (maybe-assoc {:region (region-specifier)}
               :split-point (gen-bytes)))
(defn split-region-response [] {})

(defn compact-region-request []
  (maybe-assoc {:region (region-specifier)}
               :major (gen/boolean)
               :family (gen-bytes)))
(defn compact-region-response [] {})

(defn update-favored-nodes-request []
  (letfn [(region-update-info []
            {:region (region-info)
             :favored-nodes (gen/list server-name)})]
    {:update-info (gen/list region-update-info)}))
(defn update-favored-nodes-response []
  {:response (gen/int)})

(defn merge-regions-request []
  (maybe-assoc {:region-a (region-specifier)
                :region-b (region-specifier)
                :forcible (gen/boolean)}))
(defn merge-regions-response [] {})

(defn wal-entry []
  (maybe-assoc {:key (wal-key)
                :key-value-bytes (gen/list gen-bytes)}
               :associated-cell-count (gen/int)))

(defn replicate-wal-entry-request []
  {:entry (gen/list wal-entry)})
(defn replicate-wal-entry-response [] {})

(defn roll-wal-writer-request [] {})
(defn roll-wal-writer-response [] {})

(defn stop-server-request []
  {:reason (gen/string)})
(defn stop-server-response [] {})

(defn get-server-info-request [] {})
(defn server-info []
  (maybe-assoc {:server-name (server-name)}
               :webui-port (gen/int)))
(defn get-server-info-response []
  {:server-info (server-info)})
