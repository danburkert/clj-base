(defproject clj-base "0.1.0-SNAPSHOT"
  :description "A functional HBase client"
  :url "http://www.github.com/danburkert/clj-base"
  :license {:name "Apache License, Version 2.0"
            :url "https://www.apache.org/licenses/LICENSE-2.0.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.apache.hbase/hbase-protocol "0.96.0"]
                 [io.netty/netty-all "4.0.8.Final"]
                 [org.flatland/protobuf "0.7.3-SNAPSHOT"]
                 [zookeeper-clj "0.9.1"]
                 [org.clojure/tools.logging "0.2.6"]
                 [org.clojure/test.generative "0.5.0"]
                 [org.clojure/data.generators "0.1.2"]]
  :java-source-paths ["java"]
  ;; :global-vars  {*warn-on-reflection* true}
  :aliases {"generative" ^{:doc "Run test.generative specs"}
            ["run" "-m" "clojure.test.generative.runner" "test"]
            "test-all" ^{:doc "Run all tests (clojure.test tests and test.generative specs)"}
            ["do" "test," "run" "-m" "clojure.test.generative.runner" "test"]})
