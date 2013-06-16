(ns danburkert.hbase-client.messages
  (:require [flatland.protobuf.core :as pb])
  (:import [org.apache.hadoop.hbase.protobuf.generated
            ClientProtos$Get]))

(def Get (pb/protodef ClientProtos$Get))
