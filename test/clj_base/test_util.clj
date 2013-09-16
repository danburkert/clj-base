(ns clj-base.test-util
  (:require [clojure.data.generators :as gen]
            [clj-base.messages :as msg])
  (:import [io.netty.buffer Unpooled ByteBufOutputStream]
           [io.netty.channel.embedded EmbeddedChannel]
           [io.netty.channel ChannelHandlerAdapter]
           [flatland.protobuf PersistentProtocolBufferMap]))

(defn mock-channel
  "Creates an EmbeddedChannel instance and registers the handlers on it."
  [& handlers]
  (EmbeddedChannel. (into-array ChannelHandlerAdapter handlers)))

(defn to-buf
  "Creates a ByteBuf and writes messages to it.  Messages must be byte arrays,
   strings, or protobuf messages"
  [& msgs]
  (let [buf (Unpooled/buffer)]
    (doseq [msg msgs]
      (condp = (type msg)
        String (.writeBytes buf (.getBytes msg "utf-8"))
        PersistentProtocolBufferMap (msg/write-delimited! msg (ByteBufOutputStream. buf))
        (type (byte-array 0)) (.writeBytes buf msg)))
    buf))

(defn length-prefixed-byte-buf
  "Generates a ByteBuf with a length prefix followed by random bytes"
  []
  (let [bytes (gen/byte-array gen/byte)]
    (doto (Unpooled/buffer)
      (.writeInt (alength bytes))
      (.writeBytes bytes))))
