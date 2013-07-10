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

(def-message ConnectionHeader RPCProtos$ConnectionHeader)
(def-message ServerName HBaseProtos$ServerName)
(def-message UserInformation RPCProtos$UserInformation)

(bs/def-conversion [PersistentProtocolBufferMap byte-array]
  [msg]
  (pb/protobuf-dump msg))

(bs/def-transfer [PersistentProtocolBufferMap OutputStream]
  [msg os _]
  (pb/protobuf-write os msg))

(def ^{:doc
"([type] [type m] [type k v & kvs])
    Construct a message of the given type."}
  create pb/protobuf)

(comment

  (def my-msg (create UserInformation :effectiveUser "dburkert"))

  (:effectiveUser my-msg)

  (bs/print-bytes (bs/to-byte-array my-msg))

  (:effectiveUser (bs/convert (bs/to-byte-array my-msg) UserInformation))
  (:effectiveUser (bs/convert (bs/to-byte-buffer my-msg) UserInformation))

  (bs/conversion-path ByteBuffer UserInformation)
  (bs/conversion-path InputStream UserInformation)
  (bs/conversion-path String UserInformation)

  )
