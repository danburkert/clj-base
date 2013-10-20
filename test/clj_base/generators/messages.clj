(ns clj-base.generators.messages
  "Defines utility functions for generating protobuf messages"
  (:require [clj-base.messages :as msg]
            [clojure.data.generators :as gen]))

(defn maybe-assoc
  "Like assoc, but only works some of the time."
  [map & kvs]
  (let [pairs (partition 2 kvs)]
    (reduce (fn [m [k v]]
              (if (gen/boolean)
                (assoc m k v)
                m))
            map pairs)))

(defn geometric-5 []
  (dec (gen/geometric 0.2)))

(defn gen-bytes []
  (gen/byte-array gen/byte))

(let [alpha-numeric (concat (range (int \0) (int \9))
                            (range (int \a) (int \z))
                            (range (int \A) (int \Z)))
      start-set (map byte  (conj alpha-numeric \_))
      rest-set (map byte (concat alpha-numeric [\- \_ \.]))]
  (defn hbase-identifier
    "Generate a valid hbase identifier name.  Table names must start with a
     character in [a-zA-Z0-9\\_], followed by any number of characters in
     [a-zA-Z0-9\\-\\_\\.]"
    []
    (byte-array
      (cons (rand-nth start-set) (gen/list #(rand-nth rest-set))))))
