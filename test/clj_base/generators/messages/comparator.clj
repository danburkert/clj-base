(ns clj-base.generators.messages.comparator
  "Defines test generators for the protobuf messages in ComparatorProtos.proto"
  (:require [clj-base.messages :as msg]
            [clj-base.generators.messages :refer :all]
            [clojure.data.generators :as gen])
  (:refer-clojure :exclude [comparator]))

(defn comparator []
  (maybe-assoc {:name (gen/string)}
               :serialized-comparator (gen-bytes)))

(defn byte-array-comparable []
  {:value (gen-bytes)})

(defn binary-comparator []
  {:comparable (byte-array-comparable)})

(defn binary-prefix-comparator []
  {:comparable (byte-array-comparable)})

(defn- bitwise-op []
  (rand-nth (org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos$BitComparator$BitwiseOp/values)))
(defn bit-comparator []
  {:comparable (byte-array-comparable)
   :bitwise-op (bitwise-op)})

(defn null-comparator [] {})

(defn regex-string-comparator []
  {:pattern (gen/string)
   :pattern-flags (gen/int)
   :charset (gen/string)})

(defn substring-comparator []
  {:substr (gen/string)})
