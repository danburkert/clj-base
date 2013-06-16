(ns danburkert.hbase-client.rpc
  (:require [flatland.protobuf.core :as pb]
            [clojure.java.io :as io])
  (:import [org.apache.hadoop.hbase.protobuf.generated ClientProtos$Get]
           [org.apache.hadoop.hbase.protobuf.generated RPCProtos$ConnectionHeader RPCProtos$UserInformation]
           [com.google.protobuf ByteString CodedOutputStream]
           [flatland.protobuf PersistentProtocolBufferMap]
           [java.io ByteArrayOutputStream DataOutputStream]))

(def ConnectionHeader (pb/protodef RPCProtos$ConnectionHeader))
(def UserInformation (pb/protodef RPCProtos$UserInformation))

(defn connection-preamble []
  (let [^ByteArrayOutputStream os (doto (ByteArrayOutputStream. 6)
                                    (.write (.getBytes "HBas"))
                                    (.write (byte 0))
                                    (.write (byte 80)))]
    (.toByteArray os)))

(defn connection-header []
  (let [^PersistentProtocolBufferMap
        userInfo (pb/protobuf UserInformation :effectiveUser "dburkert")
        connectionHeader (pb/protobuf ConnectionHeader {:cellBlockCodecClass
                                                        "org.apache.hadoop.hbase.codec.KeyValueCodec"
                                                        :serviceName "ClientService"
                                                        :userInfo userInfo})
        bs (pb/protobuf-dump connectionHeader)
        len (alength bs)
        os (ByteArrayOutputStream. (+ len 4))]
    (.writeInt (DataOutputStream. os) (int (alength bs)))
    (.write os bs)
    (.toByteArray os)))

(comment

  (import org.apache.hadoop.hbase.util.Bytes)
  (Bytes/toString (connection-header))

  (use 'lamina.core 'aleph.tcp 'gloss.core)

  (def ch
    (wait-for-result
      (tcp-client {:host "localhost",
                   :port 60020})))

  (do
  (enqueue ch (connection-preamble))
  (enqueue ch (connection-header)))


  (wait-for-message ch)

  (close ch)
  )
