(ns danburkert.hbase-client.zookeeper
  (:require [danburkert.hbase-client.messages :as msg]
            [zookeeper :as zk]
            [byte-streams :as bs])
  (:import [java.nio ByteBuffer]
           [java.util Arrays]))

(def ^:private magic (unchecked-byte 0xFF))

(defn- strip-metadata
  "Strips metadata and magic bytes from the data of a zNode.  The data is
   prefixed with a single magic byte, 0xFF, followed by a serialized int
   that holds the length of the subsequent metadata.  The magic bytes \"PBUF\"
   follow the metadata, and then the serialized message."
  ^ByteBuffer [^ByteBuffer data]
  (let [pos (.position data)]
    (assert (= (.get data pos) magic) "zNode contains unrecognized data format.")
    (.position data (+ pos 9 (.getInt data (inc pos)))))) ;; 9 = 0xFF + int + "PBUF"

(defn- zNode-message
  "Fetch data from a zNode and return a ByteBuffer containing the message
   contained in the zNode."
  [zk zNode]
  (-> (:data (zk/data zk zNode))
      bs/to-byte-buffer
      strip-metadata))

(defn connect!
  "Connect to a zookeeper quorum and return the connection.  Takes a map of
   options with required key :connection and optional keys :timeout-msec
   and :watcher."
  [opts]
  (apply zk/connect (:connection opts) (flatten (seq opts))))

(defn master
  "Takes a zookeeper connection and returns the Master message."
  [zk]
  (bs/convert (zNode-message zk "/hbase/master") msg/Master))

(defn meta-region-server
  "Takes a zookeeper connection and returns the MetaRegionServer message."
  [zk]
  (bs/convert (zNode-message zk "/hbase/meta-region-server") msg/MetaRegionServer))

(comment

  (def zk (connect! {:connection "localhost"}))

  (zk/children zk "/hbase")

  (master zk)
  (meta-region-server zk)
  )
