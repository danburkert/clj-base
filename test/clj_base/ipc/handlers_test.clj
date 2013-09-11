(ns clj-base.ipc.handlers-test
  (:require [clj-base.ipc.handlers :refer :all]
            [clojure.test :refer :all]
            [clojure.test.generative :refer [defspec]]
            [clojure.data.generators :as gen]
            [clojure.test.generative.runner :as runner])
  (:import
    [java.nio.charset Charset]
    [java.util Arrays]
    [io.netty.buffer Unpooled]
    [io.netty.channel.embedded EmbeddedChannel]
    [io.netty.channel ChannelHandlerAdapter]))

(defn- mock-channel
  "Creates an EmbeddedChannel instance and registers the handlers on it."
  [& handlers]
  (EmbeddedChannel. (into-array ChannelHandlerAdapter handlers)))

(defn- to-buf
  "Creates a ByteBuf and writes messages to it.  Messages must be byte arrays
   or strings."
  [& msgs]
  (let [buf (Unpooled/buffer)]
    (doseq [msg msgs]
      (if (string? msg)
        (.writeBytes buf (.getBytes msg))
        (.writeBytes buf msg)))
    buf))

(deftest preamble-handler-test
  (let [ch (mock-channel preamble-handler)
        buf (.readOutbound ch)]
    (testing "Sends preamble upon connect"
      (is (= 6 (.readableBytes buf)) "Preamble should be 6 bytes long")
      (is (= "HBas" (.toString buf 0 4 (Charset/forName "utf8"))) "Preamble should start with string \"HBas\"")
      (is (= 00 (.getByte buf 4)) "The 5th byte of the preamble should be 0")
      (is (= 80 (.getByte buf 5)) "The 6th byte of the preamble should be 80")
      (is (nil? (.readOutbound ch))) "Channel output should only contain the preamble")))

(defspec length-encoder-spec
  (fn [msg]
    "Takes a byte array message and returns a sequence of output ByteBufs"
    (let [ch (mock-channel length-encoder)
          buf (to-buf msg)]
      (.writeOutbound ch (into-array [buf]))
      (repeatedly (fn [] (.readOutbound ch)))))
  [^{:tag (byte-array byte)} msg]
  (assert (= (+ 4 (alength msg))
             (.readableBytes (first %)))
          "Channel output should be 4 bytes longer than the message")
  (assert (Arrays/equals msg (let [ary (byte-array (alength msg))]
                               (.getBytes (first %) 4 ary)
                               ary))
          "Channel output after the prefix int should equal the message")
  (assert (= (alength msg) (.getInt (first %) 0))
          "Channel output should be prefixed with length of the message")
  (assert (nil? (second %))
          "Channel should have a single outbound message"))
#_(runner/run 2 500 #'length-encoder-spec)

(defn- length-prefixed-byte-buf
  "Generates a ByteBuf with a length prefix followed by random bytes"
  []
  (let [bytes (gen/byte-array gen/byte)]
    (doto (Unpooled/buffer)
      (.writeInt (alength bytes))
      (.writeBytes bytes))))

(defspec length-decoder-spec
  (fn [buf]
    "Takes a byte buffer and feeds it through a mock-channel with a length
     decoder handler.  Returns a sequence of input messages."
    (let [ch (mock-channel (length-decoder))]
      (.writeInbound ch (into-array [(.copy buf)])) ;; .writeInbound modifies the read markers
      (repeatedly (fn [] (.readInbound ch)))))
  [^{:tag `length-prefixed-byte-buf} buf]
  (assert (= (- (.readableBytes buf) 4)
             (.readableBytes (first %)))
          "Input message should be 4 bytes longer than the input")
  (assert (= (first %) (.slice buf 4 (- (.writerIndex buf) 4)))
          "Input after the prefix int should equal the message")
  (assert (= (.getInt buf 0) (.readableBytes (first %)))
          "Output should be of the same length as the prefix")
  (assert (nil? (second %))
          "Channel should only have one input message"))
#_(runner/run 2 500 #'length-decoder-spec)

#_(run-tests)
