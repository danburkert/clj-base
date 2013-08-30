(ns danburkert.hbase-client.ipc-test
  (:require [danburkert.hbase-client.ipc :refer :all]
            [clojure.test :refer :all])
  (:import [io.netty.channel.embedded EmbeddedChannel]))

(def channel-listener @#'danburkert.hbase-client.ipc/channel-listener)
(def channel-initializer @#'danburkert.hbase-client.ipc/channel-initializer)
(def preamble-handler @#'danburkert.hbase-client.ipc/preamble-handler)
(def length-encoder @#'danburkert.hbase-client.ipc/length-encoder)

#_(EmbeddedChannel. (into-array [(channel-initializer)]) )

#_(deftest test-preamble-handler)
