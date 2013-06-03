(ns danburkert.hbase-client.protobuf
  (:require [protobuf.core :as pb])
  (:import [java.util Arrays]
           [java.io ByteArrayOutputStream]
           [java.nio.charset Charset]
           [org.apache.hadoop.hbase.protobuf.generated HBaseProtos$ServerName]))

(def ^:private ^"[B" magic (.getBytes "PBUF" (Charset/forName "UTF-8")))

(def ServerName (pb/protodef HBaseProtos$ServerName))

(defn- strip-magic
  "Strips magic bytes from array"
  [^bytes bs] (Arrays/copyOfRange bs 6 (alength bs)))

(defn deser-server-name
  "Deserialize a ServerName message."
  [^bytes bs]
  (->> bs
       strip-magic
       (pb/protobuf-load ServerName)))

(comment

  (:hostName (pb/protobuf ServerName :hostName "dcb.mbp" :port 123))

  (defn- aconcat
    "Prepend magic bytes onto byte array"
    ^bytes [^bytes a ^bytes b]
    (.. (doto (ByteArrayOutputStream.)
          (.write a)
          (.write b))
        toByteArray))

  (defn- prepend-magic
    "Prepend magic bytes onto byte array"
    ^bytes [bs] (aconcat magic bs))

  (use 'criterium.core)

  (quick-bench (strip-magic (.getBytes "PBUF-foobar")))

  (String. (prepend-magic (.getBytes "-foobar")))

  (String. (strip-magic (prepend-magic (.getBytes "fizz buzz"))))

  )
