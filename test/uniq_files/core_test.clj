(ns uniq-files.core-test
  (:require [langohr.core :as rmq]
            [langohr.channel :as lch]
            [langohr.queue :as lq]
            [langohr.consumers :as lc]
            [langohr.basic :as lb]))

(def ^{:const true}
default-exchange-name "")

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
  configure-handler2
  [channel actions name next]
  (let [{queue-name :queue-name handler :handler} (get-in actions [name])]
    (configure-handler channel queue-name handler next)))

(defn
  handler
  [function]
  (letfn [(message [payload] (String. payload "UTF-8"))]
    (fn [channel meta ^bytes payload]
      #_(println (format "[print] Received a message: %s" message))
      (function (message payload)))))

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

(def
  actions
  {:uppercase {:queue-name "langohr.examples.uppercase"
               :handler    (handler #(.toUpperCase %))
               }
   :identity {:queue-name "langohr.examples.hello-world"
              :handler (handler identity)}})

(defn
  queue-name
  [actions key]
  (get-in actions [key :queue-name]))

(defn test-send-messages
  []
  (let [message-queue (connect-to-mq)
        {channel :channel} message-queue
        queue-name (partial queue-name actions)
        queue-name-print "langohr.examples.print"
        forward-to (fn [queue-name] (partial publish-message channel queue-name))
        print-handler (handler #(println (str "MESSAGE----> " %)))]
    (println (format "[main] Connected. Channel id: %d" (.getChannelNumber channel)))
    (configure-handler channel (get-in actions [:identity :queue-name]) identity-handler (forward-to (queue-name :uppercase)))
    (configure-handler2 channel actions :uppercase (forward-to queue-name-print))
    (configure-handler channel queue-name-print print-handler)
    (doall
      (for [i (range 10)]
        (publish-message channel (get-in actions [:identity :queue-name]) (str "Hello! " i))))
    (Thread/sleep 2000)
    (println "[main] Disconnecting...")
    (disconnect-from-mq message-queue)))
