(ns clj-base.test-util
  (:require [clojure.data.generators :as gen])
  (:import [io.netty.buffer Unpooled]
           [io.netty.channel.embedded EmbeddedChannel]
           [io.netty.channel ChannelHandlerAdapter]))

(defn mock-channel
  "Creates an EmbeddedChannel instance and registers the handlers on it."
  [& handlers]
  (EmbeddedChannel. (into-array ChannelHandlerAdapter handlers)))

(defn to-buf
  "Creates a ByteBuf and writes messages to it.  Messages must be byte arrays
   or strings."
  [& msgs]
  (let [buf (Unpooled/buffer)]
    (doseq [msg msgs]
      (if (string? msg)
        (.writeBytes buf (.getBytes msg))
        (.writeBytes buf msg)))
    buf))

(defn length-prefixed-byte-buf
  "Generates a ByteBuf with a length prefix followed by random bytes"
  []
  (let [bytes (gen/byte-array gen/byte)]
    (doto (Unpooled/buffer)
      (.writeInt (alength bytes))
      (.writeBytes bytes))))

