(ns danburkert.hbase-client.ipc.handlers-test
  (:require [danburkert.hbase-client.ipc.handlers :refer :all]
            [clojure.test.generative :refer [defspec]]
            [clojure.data.generators]
            [clojure.test.generative.runner :as runner])
  (:import
    [java.nio.charset Charset]
    [io.netty.buffer Unpooled]
    [io.netty.channel.embedded EmbeddedChannel]
    [io.netty.channel ChannelHandlerAdapter]))

(defn- mock-channel
  "Creates an EmbeddedChannel instance and registers the handlers on it."
  [& handlers]
  (EmbeddedChannel. (into-array ChannelHandlerAdapter handlers)))

(defn- to-buf-array
  "Creates a ByteBuf and writes messages to it.  Messages must be byte arrays
   or strings."
  [& msgs]
  (let [buf (Unpooled/buffer)]
    (doseq [msg msgs]
      (if (string? msg)
        (.writeBytes buf (.getBytes msg))
        (.writeBytes buf msg)))
    (into-array  [buf])))

#_(deftest preamble-handler-test
  (let [ch (mock-channel preamble-handler)
        buf (.readOutbound ch)]
    (testing "Sends preamble upon connect"
      #_(is (<= 6 (.readableBytes buf)))
      (is (= "HBas" (.toString buf 0 4 (Charset/forName "utf8"))))
      (is (= 0 (.getByte buf 4)))
      (is (= 80 (.getByte buf 5))))))

#_(deftest length-encdec-test
  (let [ch (mock-channel (length-decoder) length-encoder)
        outbound-msg (.. Unpooled buffer (writeBytes (.getBytes "outbound-msg")))
        outbound-msg (.. Unpooled buffer (writeBytes (.getBytes "outbound-msg")))
        ]
    (testing "Prepends length int on outbound messages"
      (is (.writeOutbound ch (to-buf-array "outbound-message")) "Outbound write is successful")
      )))

(defspec integers-closed-over-addition
  (fn [a b] (+' a b))                    ;; input fn
  [^long a ^long b]                     ;; input spec
  (assert (neg? %)))                ;; 0 or more validator forms


#_(run-tests)

#_(
   (let [ch (mock-channel (channel-initializer))]
     (.toString (.readOutbound ch) (Charset/forName "utf8")))
)
