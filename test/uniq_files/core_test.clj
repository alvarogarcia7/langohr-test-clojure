(ns uniq-files.core-test
  (:require [langohr.core :as rmq]
            [langohr.channel :as lch]
            [langohr.queue :as lq]
            [langohr.consumers :as lc]
            [langohr.basic :as lb]))

(def ^{:const true}
default-exchange-name "")

(defn message-handler
  [ch {:keys [content-type delivery-tag type] :as meta} ^bytes payload]
  (let [message (String. payload "UTF-8")]
    #_(println (format "[consumer] Received a message: %s, delivery tag: %d, content type: %s, type: %s"
                     message delivery-tag content-type type))
    message))

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
  ([ch qname message-handler]
   (lq/declare ch qname {:exclusive false :auto-delete true})
   (lc/subscribe ch qname message-handler {:auto-ack true}))
  ([ch qname message-handler next]
   (configure-handler ch qname (comp next message-handler))))

(defn
  to-uppercase
  [ch {:keys [content-type delivery-tag type] :as meta} ^bytes payload]
  (let [message (String. payload "UTF-8")]
    #_(println (format "[to-uppercase] Received a message: %s" message))
    (.toUpperCase message)))

(defn
  print
  [ch {:keys [content-type delivery-tag type] :as meta} ^bytes payload]
  (let [message (String. payload "UTF-8")]
    #_(println (format "[print] Received a message: %s" message))
    (println (str "MESSAGE----> " message))))


(defn
  publish-message
  [ch qname payload]
  #_(println (str "sending message:" payload))
  (lb/publish
    ch
    default-exchange-name
    qname
    payload
    {:content-type "text/plain" :type "xxx"}))

(defn test-send-messages
  []
  (let [mq (connect-to-mq)
        {channel :channel} mq
        qname "langohr.examples.hello-world"
        queue-name-uppercase "langohr.examples.uppercase"
        queue-name-print "langohr.examples.print"
        forward-to (fn [queue-name] (partial publish-message channel queue-name))]
    (println (format "[main] Connected. Channel id: %d" (.getChannelNumber channel)))
    (configure-handler channel qname message-handler (forward-to queue-name-uppercase))
    (configure-handler channel queue-name-uppercase to-uppercase (forward-to queue-name-print))
    (configure-handler channel queue-name-print print)
    (doall
      (for [i (range 10)]
        (publish-message channel qname (str "Hello! " i))))
    (Thread/sleep 2000)
    (println "[main] Disconnecting...")
    (disconnect-from-mq mq)))
