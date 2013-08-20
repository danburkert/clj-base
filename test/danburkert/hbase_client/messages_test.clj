(ns danburkert.hbase-client.messages-test
  (:require [clojure.test :refer :all]
            [byte-streams :as bs]
            [danburkert.hbase-client.messages :refer :all]))

(def msg (create RequestHeader {:call-id 0
                                 :method-name "MethodName"
                                 :request-param false}))

(def serialized-bytes (bs/to-byte-array msg))

(def delimited-bytes
  (let [out (java.io.ByteArrayOutputStream.)]
    (bs/transfer msg out)
    (.toByteArray out)))

(deftest test-size-funcs
  (testing "serialized size"
    (is (= (alength serialized-bytes)
           (serialized-size msg))))
  (testing "delimited size"
    (is (= (alength delimited-bytes)
           (delimited-size msg)))))

(deftest test-serde
  (testing "non-delimited serde"
    (is (= msg (bs/convert (bs/to-byte-array msg) RequestHeader)))
    (is (= msg (bs/convert (bs/to-byte-buffer msg) RequestHeader))))
  (testing "delimited serde"
    (let [out (java.io.ByteArrayOutputStream.)]
      (bs/transfer msg out)

      )
    )
  )

(bs/convert
  (bs/to-byte-buffer msg)
  RequestHeader)

(let [out (java.io.ByteArrayOutputStream.)]
      (bs/transfer msg out)
  (bs/to-input-stream out)
  )

(comment
  (bs/print-bytes serialized-bytes)
  (bs/print-bytes delimited-bytes)
  (run-tests)
  )
