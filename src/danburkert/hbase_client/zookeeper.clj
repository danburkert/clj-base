(ns danburkert.hbase-client.zookeeper
  (:require [danburkert.hbase-client.messages :as msg]
            [zookeeper :as zk]
            [byte-streams :as bs])
  (:import [java.nio ByteBuffer]
           [java.util Arrays]))

(def ^:private magic (unchecked-byte 0xFF))

(defn- strip-metadata
  "Strips the metadata from the data of a zNode.  Metadata is written by the
   org.apache.hbase.zookeeper.RecoverableZooKeeper class."
  ^ByteBuffer [^ByteBuffer data]
  (if (or (nil? data)
          (zero? (.capacity data))
          (not= (.get data 0) magic))
    data
    (let [pos (+ (.position data) 5 (.getInt data 1))] ;; current position + magic byte + 4 byte int + metadata-length
      (.position data pos))))

(defn- strip-magic
  "Strips magic bytes (\"PBUF\") from data of a zNode."
  ^ByteBuffer [^ByteBuffer data]
  (.position data (+ (.position data) 4)))

(defn- parse-server
  "Parse data from a zNode containing server information.  Returns a map
   containing :host, :port, and :start-code"
  [data]
  (let [server-name (-> data
                        bs/to-byte-buffer
                        strip-metadata
                        strip-magic
                        (bs/convert msg/ServerName))]
    {:host (:hostName server-name)
     :port (:port server-name)
     :start-code (:startCode server-name)}))

(defn connect!
  "Connect to a zookeeper quorum and return the connection.  Takes a map of
   options with required keys :connection, and optional keys :timeout-msec
   and :watcher."
  [opts]
  (apply zk/connect (:connection opts) (flatten (seq opts))))

(defn master-server
  "Takes a zookeeper connection and returns a map including the :host, :port,
   and :start-code of the master server."
  [zk]
  (parse-server (:data (zk/data zk "/hbase/master"))))

(defn meta-region-server
  "Takes a zookeeper connection and returns a map including the :host, :port,
   and :start-code of the meta region server."
  [zk]
  (parse-server (:data (zk/data zk "/hbase/meta-region-server"))))

(comment

  (def client (connect! {:connection "localhost"}))

  (zk/children client "/hbase")

  (bs/print-bytes (:data (zk/data client "/hbase/master")))
  (alength (:data (zk/data client "/hbase/master")))
  (bs/print-bytes (:data (zk/data client "/hbase/meta-region-server")))

  (master-server client)
  (meta-region-server client)

  (master-server client)
  (meta-region-server client)



  )
