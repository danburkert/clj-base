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

(defn connection-header [service]
  "Returns a byte array containing the connection header used to connect to a
   remote hbase server."
  (let [user-info (msg/create msg/UserInformation :effective-user "dburkert")
        connection-header (msg/create msg/ConnectionHeader {:cell-block-codec-class "org.apache.hadoop.hbase.codec.KeyValueCodec"
                                                            :service-name service
                                                            :user-info user-info})
        size (msg/serialized-size connection-header)]
    (bs/to-byte-array (doto (ByteBuffer/allocate
                              (+ size 4))
                        (.putInt size)
                        (.put (bs/to-byte-array connection-header))
                        .flip))))

(defn- write-messages [msgs]
  "Write sequence of messages to output stream prefixed by the total length"
  (let [len (apply + (map msg/delimited-size msgs))
        lenbytes (doto (ByteBuffer/allocate 4) (.putInt len) .flip)
        baos (ByteArrayOutputStream. len)]
    (bs/transfer (bs/to-input-stream lenbytes) baos)
    (doseq [msg msgs]
      (bs/transfer msg baos {:delimited true}))
    (.toByteArray baos)))

(def counter (atom -1))

(defn request-header [method-name]
  "Returns an output stream containing a request header."
  (msg/create msg/RequestHeader {:call-id (swap! counter inc)
                                 :method-name method-name
                                 :request-param true}))

(defn connect! [server service]
  (doto (ch/wait-for-result
          (tcp/tcp-client server))
    (ch/enqueue connection-preamble)
    (ch/enqueue (connection-header service))))

(defn send-message! [connection method-name msg]
  (ch/enqueue
    connection
    (write-messages [(request-header) msg])))

(defn master-running? [connection]
  (send-message! connection nil))

#_(comment

  (def zk (zk/connect! {:connection "localhost"}))

  (def ch (let [server (:server (zk/meta-region-server zk))
                conn {:host (:host-name server)
                      :port (:port server)}]
            (ch/wait-for-result
              (tcp/tcp-client conn))))

  (def ch (let [server (:master (zk/master zk))
                conn {:host (:host-name server)
                      :port (:port server)}]
            (ch/wait-for-result
              (tcp/tcp-client conn))))

  (let [out (java.io.ByteArrayOutputStream.)]
    (bs/transfer (msg/create msg/RequestHeader {:call-id 0
                                                :method-name "IsMasterRunning"
                                                :request-param true})
                 out)
    (bs/print-bytes (.toByteArray out)))

    (bs/print-bytes (hex->bytes "00:00:00:17:15:08:00:1a:0f:49:73:4d:61:73:74:65:72:52:75:6e:6e:69:6e:67:20:01:00"))

  (def ch (let [server (:master (zk/master zk))
                conn {:host (:host-name server)
                      :port (:port server)}]
            (connect! conn "MasterMonitorService")))

    (ch/enqueue
      ch
      (write-messages
        [(request-header "IsMasterRunning")
         (msg/create msg/IsMasterRunningRequest {})]))

  (ch/close ch)

  (defn hex->bytes [hex]
    (apply str
           (map
             (fn [[x y]] (char (Integer/parseInt (str x y) 16)))
             (clojure.string/split hex #":"))))


  (bs/print-bytes connection-preamble)
  (bs/print-bytes (connection-header "MasterMonitorService"))
  (bs/print-bytes (request-header))

  (zk/master zk)
  (zk/meta-region-server zk)

  (def ch
    (let [meta-server (:host-name (zk/meta-region-server zk))]
      (connect! {:host (:host-name meta-server)
                 :port (:port meta-server)})))

  (def ch
    (let [meta-server (:master (zk/master zk))]
      (connect! {:host (:host-name meta-server)
                 :port (:port meta-server)})))

    (def reponse (ch/wait-for-message ch))

    (bs/convert (bs/to-input-stream (.toByteBuffer reponse 4 (.getInt reponse 0))) msg/IsMasterRunningResponse {:delimited true})
    (bs/print-bytes (.toByteBuffer reponse 4 (.getInt reponse 0)))

  (send-message! ch nil)

  (master-running? ch)

  (ch/wait-for-message ch)

  (ch/close ch)

  )
