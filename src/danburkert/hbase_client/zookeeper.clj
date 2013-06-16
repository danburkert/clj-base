(ns danburkert.hbase-client.zookeeper
  (:require [danburkert.hbase-client.protobuf :as pb]
            [zookeeper :as zk]
            [clojure.string :as s])
  (:import [java.nio ByteBuffer]
           [java.util Arrays]))

(def ^:private magic (unchecked-byte 0xFF))
(def ^:private root-region-delimiter #",")

(defn- strip-metadata
  "Strips the metadata from the data of a zNode.  Metadata is written by the
   org.apache.hbase.zookeeper.RecoverableZooKeeper class."
  ^bytes [^bytes data]
  (if (or (nil? data)
          (zero? (alength data))
          (not= (aget data 0) magic))
    data
    (let [bb (ByteBuffer/wrap data)
          offset (+ 5 (.getInt bb 1))] ;; magic byte + 4 byte int + metadata-length
      (Arrays/copyOfRange data offset (alength data)))))

(defn parse-region-server
  "Parse data from a zNode containing region server information.  Returns a map
   containing :hostName, :port and :startCode"
  [^bytes data]
  (-> data
      strip-metadata
      (pb/server-name)))

(comment

  (def client (zk/connect "localhost:2181"))

  (zk/children client "/hbase")

  (use 'criterium.core)

  (let [data (:data (zk/data client "/hbase/master"))]
    (quick-bench (:hostName (parse-region-server data))))

  (parse-region-server (:data (zk/data client "/hbase/master")))

  (parse-region-server (:data (zk/data client "/hbase/meta-region-server")))

  )
