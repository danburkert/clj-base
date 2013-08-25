(ns danburkert.hbase-client.messages
  (:require [flatland.protobuf.core :as pb])
  (:import [flatland.protobuf PersistentProtocolBufferMap PersistentProtocolBufferMap$Def]
           [java.io InputStream OutputStream])
  (:refer-clojure :exclude [read]))

(defmacro def-message
  "Declare a protobuf message type"
  [name class]
  (let [full-class (symbol (str "org.apache.hadoop.hbase.protobuf.generated." class))]
    `(def ~name (pb/protodef ~full-class))))

;; Zookeeper messages
(def-message Master ZooKeeperProtos$Master)
(def-message MetaRegionServer ZooKeeperProtos$MetaRegionServer)
(def-message ServerName HBaseProtos$ServerName)

;; RPC messages
(def-message ConnectionHeader RPCProtos$ConnectionHeader)
(def-message UserInformation RPCProtos$UserInformation)
(def-message RequestHeader RPCProtos$RequestHeader)
(def-message ResponseHeader RPCProtos$ResponseHeader)

;; Master messages
(def-message IsMasterRunningRequest MasterProtos$IsMasterRunningRequest)
(def-message IsMasterRunningResponse MasterProtos$IsMasterRunningResponse)

;; Client messages

(def create
  "([type] [type m] [type k v & kvs])
    Construct a message of the given type."
  pb/protobuf)

(def size
  "([msg])
    Returns the serialized size of the message"
  pb/serialized-size)

(def delimited-size
  "([msg])
    Returns the delimited size of the message."
  pb/delimited-size)

(defn write!
  "Serialize the message to the outputstream without a varint delimiter"
  [^PersistentProtocolBufferMap msg ^OutputStream os]
  (.writeTo msg os))

(defn write-delimited!
  "Serialize the message to the outputstream with a varint delimiter"
  [^PersistentProtocolBufferMap msg os]
  (.writeDelimitedTo msg os))

(defn read!
  "Read a non-delimited message of the given type from the input stream"
  [type is]
  (PersistentProtocolBufferMap/parseFrom ^PersistentProtocolBufferMap$Def type ^InputStream is))

(defn read-delimited!
  "Read a delimited message of the given type from the input stream"
  [type is]
  (PersistentProtocolBufferMap/parseDelimitedFrom type is))

(comment

  (type (java.nio.ByteBuffer/wrap (byte-array 1)))

  (def msg (create RequestHeader {:call-id 0
                                  :method-name "MethodName"
                                  :request-param false}))

  (def serialized-bytes
    (let [out (java.io.ByteArrayOutputStream.)]
      (write! msg out)
      (.toByteArray out)))

  (read serialized-bytes RequestHeader)

  )
