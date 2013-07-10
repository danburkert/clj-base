(ns danburkert.hbase-client.rpc
  (:require [danburkert.hbase-client.messages :as msg]
            [byte-streams :as bs]
            [lamina.core :as lam]
            [aleph.tcp :as tcp])
  (:import [java.io ByteArrayOutputStream DataOutputStream]
           [java.nio ByteBuffer]))

(def ^:private connection-preamble
  (bs/to-byte-array (doto (ByteBuffer/allocate 6)
                      (.put (.getBytes "HBas"))
                      (.put (byte 0))
                      (.put (byte 80))
                      .flip)))

(defn connection-header []
  (let [user-info (msg/create msg/UserInformation :effectiveUser "dburkert")
        connection-header (msg/create msg/ConnectionHeader {:cellBlockCodecClass
                                                            "org.apache.hadoop.hbase.codec.KeyValueCodec"
                                                            :serviceName "ClientService"
                                                            :userInfo user-info})
        msg-bytes (bs/to-byte-array connection-header)
        len (.length msg-bytes)]
    (bs/to-byte-array (doto (ByteBuffer/allocate (len + 4))
                        (.putInt len)
                        (.put msg-bytes)
                        .flip))))

(defn connect! [server]
  (doto (lam/wait-for-result
          (tcp/tcp-client server))
    (lam/enqueue connection-preamble)
    (lam/enqueue (connection-header))))

(comment

  (bs/print-bytes (connection-header))

  (use 'lamina.core 'aleph.tcp 'gloss.core)

  (def ch (connect! {:host "localhost"
                     :port 60020 }))

  (wait-for-message ch)

  (close ch)

  )
