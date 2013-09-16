(ns clj-base.ipc.handlers-test
  (:require [clj-base.ipc.handlers :refer :all]
            [clj-base.test-util :refer :all]
            [clj-base.messages :as msg]
            [clojure.test :refer :all]
            [clojure.test.generative :refer [defspec]]
            [clojure.data.generators :as gen]
            [clojure.test.generative.runner :as runner])
  (:import
    [java.nio.charset Charset]
    [java.util Arrays]
    [io.netty.buffer Unpooled ByteBufInputStream]
    [io.netty.channel.embedded EmbeddedChannel]
    [io.netty.channel ChannelHandlerAdapter]))

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



(deftest preamble-handler-test
  (let [ch (mock-channel preamble-handler)
        buf (.readOutbound ch)]
    (testing "Sends preamble upon connect"
      (is (= 6 (.readableBytes buf)) "Preamble should be 6 bytes long")
      (is (= "HBas" (.toString buf 0 4 (Charset/forName "utf8"))) "Preamble should start with string \"HBas\"")
      (is (= 00 (.getByte buf 4)) "The 5th byte of the preamble should be 0")
      (is (= 80 (.getByte buf 5)) "The 6th byte of the preamble should be 80")
      (is (nil? (.readOutbound ch))) "Channel output should only contain the preamble")))

(deftest rpc-codec-test
  (let [codec (RpcCodec.)
        state (.state codec)]
    (testing "RpcCodec initial state"
      (is (= -1 (:counter @state)))
      (is (empty? (:requests @state))))))

(defn- test-rpc-codec
  "Test the request / response cycle of RpcCodec"
  [method request response]
  (let [codec (RpcCodec.)
        state (.state codec)
        ch (mock-channel codec)
        id (inc (:counter @state))
        p (promise)
        request-type (msg/request-type method)
        response-type (msg/response-type method)
        method-name (msg/method-name method)
        ]
    (testing (str "RpcCodec " method " request")
      (is (nil? (.readOutbound ch)) "Outbound channel should be empty after initialization")
      (.writeOutbound ch (to-array [{:method :master-running?
                                     :promise p
                                     :request request}]))
      (is (= id (:counter @state)) "call-id counter should be incremented after sending a message")
      (is (= p (get-in @state [:requests 0 :promise])) "codec should keep reference to promise")
      (is (= response-type (get-in @state [:requests 0 :response-type]))
          "codec should store rpc method return type")
      (let [buf (.readOutbound ch)
            input (ByteBufInputStream. buf)]
        (is (= (msg/create msg/RequestHeader {:call-id id :method-name method-name :request-param true})
               (msg/read-delimited! msg/RequestHeader input))
            "Header should be serialized to output")
        (is (= request (msg/read-delimited! request-type input))
            "Request should be serialized to output"))
      (is (nil? (.readOutbound ch)) "Outbound channel should be empty after reading only write"))
    (testing (str "RpcCodec " method " response")
      (is (nil? (.readInbound ch)) "Inbound channel should be empty after initialization")
      (let [header (msg/create msg/ResponseHeader {:call-id id})
            buf (to-buf header response)]
        (.writeInbound ch (to-array [buf]))
        (is (= {:response response} @p) "Promise should be fulfilled with response message")
        (is (empty? (:requests @state)) "Request should be removed from state after fulfilling promise")
        (is (nil? (.readInbound ch)) "Inbound channel should be empty")))))

(deftest is-master-running-rpc-test
  (test-rpc-codec :master-running?
                  (msg/create msg/IsMasterRunningRequest {})
                  (msg/create msg/IsMasterRunningResponse {:is-master-running true})))


#_(run-tests)

#_(runner/run 2 500 #'length-decoder-spec)
