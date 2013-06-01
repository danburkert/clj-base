(ns danburkert.hbase-client.zookeeper
  (:require [zookeeper :as zk]
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

(defn- parse-root-region
  "Parse data from the root region server zNode and returns a 3-tuple of
   [hostname, port, startcode]"
  [^bytes data]
  (s/split (String. (strip-metadata data))
           root-region-delimiter))

(comment

(def client (zk/connect "localhost:2181"))

(strip-metadata (:data (zk/data client "/hbase/root-region-server")))

(parse-root-region (:data (zk/data client "/hbase/root-region-server")))

  )
