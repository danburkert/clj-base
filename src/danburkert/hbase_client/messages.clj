(ns danburkert.hbase-client.messages
  (:require [flatland.protobuf.core :as pb]
            [byte-streams :as bs]
            [clojure.java.io :as io])
  (:import [java.io InputStream OutputStream]
           [java.nio ByteBuffer]
           [flatland.protobuf PersistentProtocolBufferMap]
           [com.google.protobuf CodedInputStream]))

(def ^:private byte-array (class (clojure.core/byte-array 0)))

(defmacro def-message
  "Defines a message type.  Sets up the protobuf definition and necessary byte
   conversion paths."
  [name class]
  (let [full-class (symbol (str "org.apache.hadoop.hbase.protobuf.generated." class))]
    `(do
       (def ~name (pb/protodef ~full-class))
       (bs/def-conversion [byte-array ~name]
         [ary#]
         (PersistentProtocolBufferMap/create ~name ary#))
       (bs/def-conversion [ByteBuffer ~name]
         [^ByteBuffer buf#]
         (let [input# (CodedInputStream/newInstance (.array buf#)
                                                    (.position buf#)
                                                    (.remaining buf#))]
           (PersistentProtocolBufferMap/parseFrom ~name input#)))
       (bs/def-conversion [~'InputStream ~name]
         [input-stream# {:keys [delimited#] :or {delimited# true}}]
         (if delimited#
           (PersistentProtocolBufferMap/parseDelimitedFrom ~name input-stream#)
           (PersistentProtocolBufferMap/parseFrom ~name (CodedInputStream/newInstance input-stream#)))
         (pb/protobuf-load-stream ~name input-stream#))
       (bs/def-conversion [~'InputStream (bs/seq-of ~name)]
         [input-stream#]
         (pb/protobuf-seq ~name input-stream#)))))

;; Zookeeper messages
(def-message Master ZooKeeperProtos$Master)
(def-message MetaRegionServer ZooKeeperProtos$MetaRegionServer)
(def-message ServerName HBaseProtos$ServerName)

;; RPC messages
(def-message ConnectionHeader RPCProtos$ConnectionHeader)
(def-message UserInformation RPCProtos$UserInformation)
(def-message RequestHeader RPCProtos$RequestHeader)
(def-message ResponseHeader RPCProtos$ResponseHeader)

;; Client messages

(bs/def-conversion [PersistentProtocolBufferMap byte-array]
  [msg]
  (pb/protobuf-dump msg))

(bs/def-transfer [PersistentProtocolBufferMap OutputStream]
  [msg os {:keys [delimited] :or {delimited true}}]
  (if delimited
    (.writeDelimitedTo msg os)
    (.writeTo msg os)))

(def ^{:doc
"([type] [type m] [type k v & kvs])
    Construct a message of the given type."}
  create pb/protobuf)

(def ^{:doc
"([msg]
    Returns the serialized size of the message."}
  serialized-size pb/serialized-size)

(comment

  (use 'criterium.core)

  (def my-msg (create UserInformation :effective-user "dburkert"))

  (bs/print-bytes (bs/to-byte-array my-msg))

  (bs/convert (bs/to-byte-array my-msg) UserInformation)
  (bs/convert (bs/to-byte-buffer my-msg) UserInformation)

  (quick-bench (bs/convert (bs/to-byte-array my-msg) UserInformation)) ;;168 micro seconds
  (quick-bench (bs/convert (bs/to-byte-buffer my-msg) UserInformation))

  (quick-bench (bs/to-byte-array my-msg)) ;; 80 micro seconds
  (quick-bench (bs/to-byte-buffer my-msg)) ;; 98 micro seconds
  (quick-bench (bs/to-input-stream my-msg)) ;; 108 micro seconds
  (quick-bench (bs/to-readable-channel my-msg)) ;; 154 micro seconds

  (let [msg (bs/to-byte-array my-msg)] (quick-bench (bs/convert msg UserInformation))) ;; 82 micro seconds
  (let [msg (bs/to-byte-buffer my-msg)] (quick-bench (bs/convert msg UserInformation))) ;; 88 micro seconds

  (bs/conversion-path ByteBuffer UserInformation)
  (bs/conversion-path InputStream UserInformation)
  (bs/conversion-path String UserInformation)

  (to-byte-array
    (doto (ByteBuffer/allocate 8)
      (.putLong 13)
      .flip))
  )
