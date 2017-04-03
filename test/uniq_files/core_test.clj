(ns uniq-files.core-test
  (:require [langohr.core :as rmq]
            [langohr.channel :as lch]
            [langohr.queue :as lq]
            [langohr.consumers :as lc]
            [langohr.basic :as lb]
            [uniq-files.core :refer :all]))

(def ^{:const true}
default-exchange-name "")

(defn message-handler
  [ch {:keys [content-type delivery-tag type] :as meta} ^bytes payload]
  (println (format "[consumer] Received a message: %s, delivery tag: %d, content type: %s, type: %s"
                   (String. payload "UTF-8") delivery-tag content-type type)))

(defn
  connect-to-mq
  []
  (let [conn (rmq/connect)
        ch (lch/open conn)]
    {:connection conn
     :channel    ch}))

(defn
  disconnect-from-mq
  [mq]
  (let [{conn :connection ch :channel} mq]
    (rmq/close ch)
    (rmq/close conn)))

(defn configure-handler
  [ch qname message-handler]
  (lq/declare ch qname {:exclusive false :auto-delete true})
  (lc/subscribe ch qname message-handler {:auto-ack true}))

(defn
  get-last
  [_ ^bytes payload]
  (println (format "[get-last] Received a message: %s" (String. payload "UTF-8"))))


(defn
  publish-message
  [ch qname message-type payload]
  (lb/publish
    ch
    default-exchange-name
    qname
    payload
    {:content-type "text/plain" :type "greetings.hi"}))

(defn test-send-messages
  []
  (let [mq (connect-to-mq)
        {ch :channel} mq
        qname "langohr.examples.hello-world"]
    (println (format "[main] Connected. Channel id: %d" (.getChannelNumber ch)))
    (configure-handler ch qname message-handler)
    (doall
      (for [i (range 10)]
             (lb/publish ch default-exchange-name qname (str "Hello! " i) {:content-type "text/plain" :type
                                                                                         "greetings.hi"})))
    (Thread/sleep 2000)
    (println "[main] Disconnecting...")
    (disconnect-from-mq mq)))
