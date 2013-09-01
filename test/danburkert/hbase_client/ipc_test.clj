(ns danburkert.hbase-client.ipc-test
  (:require [danburkert.hbase-client.ipc :refer :all]
            [clojure.test :refer :all])
  (:import [io.netty.channel.embedded EmbeddedChannel]))
