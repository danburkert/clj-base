(ns danburkert.hbase-client.rpc
  (:require [danburkert.hbase-client.messages :as msg]
            [danburkert.hbase-client.zookeeper :as zk]
            [byte-streams :as bs]
            [lamina.core :as ch]
            [aleph.tcp :as tcp])
  (:import [java.io ByteArrayOutputStream DataOutputStream]
           [java.nio ByteBuffer]))

(def ^:private connection-preamble
  (bs/to-byte-array (doto (ByteBuffer/allocate 6)
                      (.put (bs/to-byte-array "HBas"))
                      (.put (byte 0))
                      (.put (byte 80))
                      .flip)))

(defn connection-header []
  "Returns a byte array containing the connection header used to connect to a
   remote hbase server."
  (let [user-info (msg/create msg/UserInformation :effective-user "dburkert")
        connection-header (msg/create msg/ConnectionHeader {:cell-block-codec-class
                                                            "org.apache.hadoop.hbase.codec.KeyValueCodec"
                                                            :service-name "ClientService"
                                                            :user-info user-info})
        size (msg/serialized-size connection-header)]
    (bs/to-byte-array (doto (ByteBuffer/allocate
                              (+ size 4))
                        (.putInt size)
                        (.put (bs/to-byte-array connection-header))
                        .flip))))

(defn- write-messages [msgs & {:keys [delimited] :or {delimited true} :as opts}]
  "Write sequence of messages to output stream prefixed by the total length"
  (let [len (reduce + (map msg/serialized-size msgs))
        baos (ByteArrayOutputStream. len)]
    (bs/transfer (int len) baos)
    (doseq [msg msgs]
      (bs/transfer msg baos))
    baos))

(bs/print-bytes (write-messages []))

(bs/conversion-path (int 13) byte-array)

(bs/convert (int 13) ByteBuffer)

(defn request-header []
  "Returns an output stream containing a request header."
  )

(defn create-request [msg]
  "Returns a byte array containing the serialized message request."
  )

(defn connect! [server]
  (doto (ch/wait-for-result
          (tcp/tcp-client server))
    (ch/enqueue connection-preamble)
    (ch/enqueue (connection-header))))

(defn send-message! [connection msg]
  (ch/enqueue )
  )

(comment

  (def zk (zk/connect! {:connection "localhost"}))

  (bs/print-bytes (connection-header))

  (def ch
    (let [meta-server (:server (zk/meta-region-server zk))]
      (connect! {:host (:host-name meta-server)
                 :port (:port meta-server)})))

  (ch/wait-for-message ch)

  (ch/close ch)

  )
