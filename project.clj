(defproject danburkert/hbase-client "0.1.0-SNAPSHOT"
  :description "A better HBase client"
  :url "http://www.github.com/danburkert/hbase-client"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.apache.hbase/hbase-protocol "0.95.2-SNAPSHOT"]
                 [org.clojure/clojure "1.5.1"]
                 [org.flatland/protobuf "0.7.3-SNAPSHOT"]
                 [zookeeper-clj "0.9.1"]
                 [aleph "0.3.0-rc2"]
                 [lamina "0.5.0-rc3"]
                 [byte-streams "0.1.5-SNAPSHOT"]]
  :global-vars  {*warn-on-reflection* true})
