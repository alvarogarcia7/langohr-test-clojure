(ns uniq-files.core-test
  (:require [langohr.core :as rmq]
            [langohr.channel :as lch]
            [langohr.queue :as lq]
            [langohr.consumers :as lc]
            [langohr.basic :as lb]
            [uniq-files.core :refer :all]
            [clojure.string :require :split]))

(def ^{:const true}
default-exchange-name "")

(defn message-handler
  [ch {:keys [content-type delivery-tag type] :as meta} ^bytes payload]
  (println (format "[consumer] Received a message: %s, delivery tag: %d, content type: %s, type: %s"
                   (String. payload "UTF-8") delivery-tag content-type type)))

(defn
  group-by-hash
  [lines]
  (letfn [(tokenize
            [line]
            (let [[filename hash] (split line #" ")]
              {:hash hash :filename filename}))]
    (->>
      lines
      (map tokenize)
      (reduce (fn
                [acc ele]
                (let [{hash :hash filename :filename} ele
                      current (get acc hash [])]
                  (assoc acc hash (conj current filename)))) {})
      )))


(def lines
  (with-open [rdr (reader "/tmp/md5sc_sorted.txt")]
    (doall (line-seq rdr))))

(defn
  connect-to-mq
  []
  (let [conn (rmq/connect)
        ch (lch/open conn)]
    {:connection conn
     :channel    ch}))

(close
  [])

(defn test-send-messages
  []
  (let [{conn :connection ch :channel} (connect-to-mq)
        qname "langohr.examples.hello-world"]
    (println (format "[main] Connected. Channel id: %d" (.getChannelNumber ch)))
    (lq/declare ch qname {:exclusive false :auto-delete true})
    (lc/subscribe ch qname message-handler {:auto-ack true})
    (doall
      (for [i (range 10)]
             (lb/publish ch default-exchange-name qname (str "Hello! " i) {:content-type "text/plain" :type
                                                                                         "greetings.hi"})))
    (Thread/sleep 2000)
    (println "[main] Disconnecting...")
    (rmq/close ch)
    (rmq/close conn)))
