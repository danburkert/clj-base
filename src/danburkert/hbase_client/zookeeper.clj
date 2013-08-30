(ns danburkert.hbase-client.zookeeper
  (:require [danburkert.hbase-client :refer :all]
            [danburkert.hbase-client.messages :as msg]
            [zookeeper :as zk]
            [clojure.tools.logging :as log])
  (:import [java.io ByteArrayInputStream]
           [java.nio Buffer ByteBuffer]))

(def ^:private magic (unchecked-byte 0xFF))

(defn- strip-metadata
  "Returns an input stream wrapping the passed in data with the position offset
   to the index of the message.  The data is prefixed with a single magic byte,
   0xFF, followed by a serialized int that holds the length of the subsequent
   metadata.  The magic bytes \"PBUF\" follow the metadata, and finally the
   serialized message."
  [^bytes data]
  (assert (= (aget data 0) magic) "zNode contains unrecognized data format")
  (let [offset (+ 9 (-> data ByteBuffer/wrap (.getInt 1))) ;; 9 = 0xFF + int + "PBUF"
        length (- (alength data) offset)]
    (ByteArrayInputStream. data offset length)))

(defn- zNode-message-stream
  "Fetch data from a zNode and return a ByteBuffer containing the serialized
   message contained in the zNode."
  [zk zNode]
  (strip-metadata (:data (zk/data zk zNode))))

(defrecord Zookeeper [client opts]
  ZookeeperService
  (master [this]
    (msg/read! msg/Master (zNode-message-stream
                            client (str (:znode-parent opts) "/master"))))
  (meta-region-server [this]
    (msg/read! msg/MetaRegionServer
               (zNode-message-stream
                 client (str (:znode-parent opts) "/meta-region-server"))))
  Closeable
  (close [this] (zk/close client)))

(def ^:private default-opts
  {:host "localhost"
   :port 2181
   :znode-parent "/hbase"})

(defn zookeeper
  "Creates a ZookeeperService connected to the specified quorum"
  [opts]
  (let [{:keys [host port] :as opts} (merge default-opts opts)
        conn (zk/connect (str host ":" port))]
    (->Zookeeper conn opts)))

(comment
  (def zk (zookeeper {}))
  (pr zk)
  (master zk)
  (meta-region-server zk)
  (close zk)

  (zk/children (:client zk) "/hbase")
  )
