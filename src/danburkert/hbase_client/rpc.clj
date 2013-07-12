(ns danburkert.hbase-client.rpc
  (:require [danburkert.hbase-client.messages :as msg]
            [danburkert.hbase-client.zookeeper :as zk]
            [byte-streams :as bs]
            [lamina.core :as lam]
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
  (let [user-info (msg/create msg/UserInformation :effective-user "dburkert")
        connection-header (msg/create msg/ConnectionHeader {:cell-block-codec-class
                                                            "org.apache.hadoop.hbase.codec.KeyValueCodec"
                                                            :service-name "ClientService"
                                                            :user-info user-info})
        msg-bytes (bs/to-byte-array connection-header)
        len (alength msg-bytes)]
    (bs/to-byte-array (doto (ByteBuffer/allocate (+ len 4))
                        (.putInt len)
                        (.put msg-bytes)
                        .flip))))

(defn connect! [server]
  (doto (lam/wait-for-result
          (tcp/tcp-client server))
    (lam/enqueue connection-preamble)
    (lam/enqueue (connection-header))))


(comment

  (def zk (zk/connect! {:connection "localhost"}))

  (bs/print-bytes (connection-header))

  (def ch
    (let [meta-server (:server (zk/meta-region-server zk))]
      (connect! {:host (:host-name meta-server)
                 :port (:port meta-server)})))

  (lam/wait-for-message ch)

  (lam/close ch)

  )
