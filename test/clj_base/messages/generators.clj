(ns clj-base.messages.generators
  "Defines test generators for protobuf messages"
  (:require [clj-base.messages :as msg]
            [clojure.data.generators :as gen]))

(defn- gen-enum [enum]
  (let [values (. (resolve enum) values)]
    (aget values (gen/uniform 0 (alength values)))))

(defn- geometric-5 []
  (dec (gen/geometric 0.2)))

(defn- gen-bytes []
  (gen/byte-array gen/byte))

;; HBaseProtos
(defn- gen-table-name
  "Generate a valid hbase table name.  Table names must start with a character
   in [a-zA-Z0-9\\_], followed by any number of characters in [a-zA-Z0-9\\-\\_\\.]"
  []
  (let [alpha-numeric (concat (range (int \0) (int \9))
                              (range (int \a) (int \z))
                              (range (int \A) (int \Z)))
        start-set (conj alpha-numeric \_)
        rest-set (concat alpha-numeric [\- \_ \.])]
    (byte-array
      (map byte
           (cons (rand-nth start-set) (gen/list #(rand-nth rest-set)))))))

(defn gen-permission []
  (letfn [(gen-type []
            (rand-nth (org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos$Permission$Type/values)))
          (gen-actions []
            (gen/list #(rand-nth (org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos$Permission$Action/values))
                      geometric-5))
          (gen-global-permission []
            {:action (gen-actions)})
          (gen-namespace-permission []
            (cond-> {:action (gen-actions)}
              (gen/boolean) (assoc :namespace-name (gen-bytes))))
          (gen-table-permission []
            (cond-> {:action (gen-actions)}
              (gen/boolean) (assoc :table-name (gen-table-name))
              (gen/boolean) (assoc :family (gen-bytes))
              (gen/boolean) (assoc :family (gen-bytes))))]
    (cond-> {:type (gen-type)}
      (gen/boolean) (assoc :global-permission (gen-global-permission))
      (gen/boolean) (assoc :namespace-permission (gen-namespace-permission))
      (gen/boolean) (assoc :table-permission (gen-table-permission)))))

#_(gen-permission)
