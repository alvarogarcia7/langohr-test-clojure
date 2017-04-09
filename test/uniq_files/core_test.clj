(ns uniq-files.core-test
  (:require [langohr.core :as rmq]
            [langohr.channel :as lch]
            [langohr.queue :as lq]
            [langohr.consumers :as lc]
            [langohr.basic :as lb]))

(def ^{:const true}
default-exchange-name "")

(defn identity-handler
  [channel {:keys [content-type delivery-tag type] :as meta} ^bytes payload]
  (let [message (String. payload "UTF-8")]
    #_(println (format "[consumer] Received a message: %s, delivery tag: %d, content type: %s, type: %s"
                     message delivery-tag content-type type))
    message))

(defn
  connect-to-mq
  []
  (let [connection (rmq/connect)
        channel (lch/open connection)]
    {:connection connection
     :channel    channel}))

(defn
  disconnect-from-mq
  [message-queue]
  (let [{connection :connection channel :channel} message-queue]
    (rmq/close channel)
    (rmq/close connection)))

(defn configure-handler
  ([channel queue-name message-handler]
   (lq/declare channel queue-name {:exclusive false :auto-delete true})
   (lc/subscribe channel queue-name message-handler {:auto-ack true}))
  ([channel queue-name message-handler next]
   (configure-handler channel queue-name (comp next message-handler))))

(defn
  to-uppercase-handler
  [_ _ ^bytes payload]
  (let [message (String. payload "UTF-8")]
    #_(println (format "[to-uppercase] Received a message: %s" message))
    (.toUpperCase message)))

(defn
  make-print-handler
  []
  (letfn [(message [payload] (String. payload "UTF-8"))]
    (fn [_ _ ^bytes payload]
      #_(println (format "[print] Received a message: %s" message))
      (println (str "MESSAGE----> " (message payload))))))


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
  (let [message-queue (connect-to-mq)
        {channel :channel} message-queue
        qname "langohr.examples.hello-world"
        queue-name-uppercase "langohr.examples.uppercase"
        queue-name-print "langohr.examples.print"
        forward-to (fn [queue-name] (partial publish-message channel queue-name))]
    (println (format "[main] Connected. Channel id: %d" (.getChannelNumber channel)))
    (configure-handler channel qname identity-handler (forward-to queue-name-uppercase))
    (configure-handler channel queue-name-uppercase to-uppercase-handler (forward-to queue-name-print))
    (configure-handler channel queue-name-print (make-print-handler))
    (doall
      (for [i (range 10)]
        (publish-message channel qname (str "Hello! " i))))
    (Thread/sleep 2000)
    (println "[main] Disconnecting...")
    (disconnect-from-mq message-queue)))
