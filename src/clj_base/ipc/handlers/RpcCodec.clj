(ns clj-base.ipc.handlers.RpcCodec
  (:require [clj-base.messages :as msg])
  (:import [io.netty.handler.codec ByteToMessageCodec]
           [io.netty.buffer ByteBufOutputStream])
  (:gen-class :extends ByteToMessageCodec
              :init init
              :constructors {[] []}
              :state state
              :main false))

(defn -init []
  [[] [(atom {:counter -1
              :promises {}})]])

#_(defn -encode
  "Takes a request map, and serializes it to the channel.  The request map may
   contain the following keys:
    :method -> (required) the RPC method being called
    :request -> (optional) the request message
    :cells -> (optional) a sequence of Cells to be sent with the request
    :promise -> (optional) a promise that the response will be delivered to"
  [this ctx msg buf]
  {:pre [(string? (:method msg))]}
  (let [{:keys [counter promises]} (.state this)
        id (swap! counter inc)
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
        (swap! promises assoc id promise)))))

#_(defn -decode
  "Takes a serialized response, deserializes it, and delivers it to the
   associated future, if it exists.  The delivered response is a map containing
   the following:
    :response -> (optional) the response message
    :exeption -> (optional) the exception message, if the request triggered an exception
    :cells -> (optional) sequence of cells returned with the response"
  [this buf out]
  (with-open [in (ByteBufInputStream. buf)]
    (let [{:keys [promises]} (.state this)
          header (msg/read-delimited! msg/ResponseHeader in)]
      (cond-> {}
        (contains? header :exception) (assoc :exception (:exception header))
        (not (contains? header :exception))
        (assoc :response (msg/read-delimited! in ))
        )

      )
    )
  )
