(ns clj-base.ipc.handlers.RpcCodec
  (:require [clj-base.messages :as msg]
            [clojure.tools.logging :as log])
  (:import [io.netty.handler.codec ByteToMessageCodec]
           [io.netty.buffer ByteBufInputStream ByteBufOutputStream]
           [java.io IOException])
  (:gen-class :extends ByteToMessageCodec
              :init init
              :constructors {[] []}
              :state state
              :main false))

(defn -init []
  [[] [(atom {:counter -1
              :requests {}})]])

(defn -encode
  "Takes a request map, and serializes it to the channel.  The request map may
   contain the following keys:
    :method -> (required) the RPC method being called
    :request -> (optional) the request message
    :cells -> (optional) a sequence of Cells to be sent with the request
    :promise -> (optional) a promise that the response will be delivered to"
  [this ctx msg buf]
  (let [id (:counter (swap! (.state this) update-in [:counter] inc))
        header (msg/create msg/RequestHeader
                           {:call-id id
                            :method-name (:method msg)
                            :request-param (contains? msg :request)
                            :cell-block-meta (contains? msg :cells)})]
    (with-open [out (ByteBufOutputStream. buf)]
      (msg/write-delimited! header out)
      (if-let [request (:request msg)]
        (msg/write-delimited! request out))
      #_(if-let [cells (:cells msg)]
        (msg/write-delimited! cells out))
      (if-let [promise (:promise msg)]
        (swap! (.state this) assoc-in [:requests id] {:promise promise
                                              :response-type (msg/response-type (:method msg))})))))

(defn -decode
  "Takes a serialized response, deserializes it, and delivers it to the
   associated future, if it exists.  The delivered response is a map containing
   the following:
    :response -> (optional) the response message
    :exeption -> (optional) the exception message, if the request triggered an exception
    :cells -> (optional) sequence of cells returned with the response"

  [this buf out]
  (with-open [in (ByteBufInputStream. buf)]
    (if-let [header (msg/read-delimited! msg/ResponseHeader in)]
      (if-let [id (:call-id header)]
        (if-let [{:keys [promise response-type]} (get-in @(.state this) [:requests id])]
          (do
            (swap! (.state this) dissoc :requests id)
            (deliver promise
                     (cond-> (if (contains? header :exception)
                               {:exception (:exception header)}
                               {:response (msg/read-delimited! response-type in)})
                       (contains? header :cell-block-meta)
                       (assoc :cells (.readSlice buf (:length (:cell-block-meta header)))))))
          (log/error "No request registered for response" header))
        (log/error "Response header does not contain id field" header))
      (log/error "Unable to parse header from response"))))
