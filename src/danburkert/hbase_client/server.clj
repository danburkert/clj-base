(ns danburkert.hbase-client.server
  )

(comment
(use 'lamina.core 'aleph.tcp 'gloss.core)

(defn handler [ch client-info]
  (receive-all ch
    #(enqueue ch (str "You said " %))))

(start-tcp-server handler {:port 10000, :frame (string :utf-8 :delimiters ["\r\n"])})

(def ch
  (wait-for-result
    (tcp-client {:host "localhost",
                 :port 10000,
                 :frame (string :utf-8 :delimiters ["\r\n"])})))


(enqueue ch "Hello, server! 2")

(wait-for-message ch)
  )
