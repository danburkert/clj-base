(ns danburkert.hbase-client.zookeeper
  (:require [danburkert.hbase-client.messages :as msg]
            [zookeeper :as zk]
            [clojure.tools.logging :as log])
  (:import [java.nio Buffer ByteBuffer]))

(def ^:private magic (unchecked-byte 0xFF))

(defn- strip-metadata
  "Returns a ByteBuffer wrapping the passed in data with the position offset to
   the index of the message.  The data is prefixed with a single magic byte,
   0xFF, followed by a serialized int that holds the length of the subsequent
   metadata.  The magic bytes \"PBUF\" follow the metadata, and finally the
   serialized message."
  [^bytes data]
  (assert (= (aget data 0) magic) "zNode contains unrecognized data format")
  (let [^ByteBuffer buf (ByteBuffer/wrap data)
        offset (.getInt buf 1)]
    (.position buf (+ 9 offset))
    buf)) ;; 9 = 0xFF + int + "PBUF"

(defn- zNode-message
  "Fetch data from a zNode and return a ByteBuffer containing the serialized
   message contained in the zNode."
  [zk zNode]
  (strip-metadata (:data (zk/data zk zNode))))

(defn connect!
  "Connect to a zookeeper quorum and return the connection.  Takes a map of
   options with required key :connection and optional keys :timeout-msec
   and :watcher."
  [opts]
  (apply zk/connect (:connection opts) (flatten (seq opts))))

(defn master
  "Takes a zookeeper connection and returns the Master message."
  [zk]
  (msg/read msg/Master (zNode-message zk "/hbase/master")))

(defn meta-region-server
  "Takes a zookeeper connection and returns the MetaRegionServer message."
  [zk]
  (msg/read msg/MetaRegionServer (zNode-message zk "/hbase/meta-region-server")))

(comment

  (def zk (connect! {:connection "localhost"}))

  (zk/children zk "/hbase")

  (:data (zk/data zk "/hbase/meta-region-server"))
  (:data (zk/data zk "/hbase/master"))

  (master zk)
  (meta-region-server zk)

  )
