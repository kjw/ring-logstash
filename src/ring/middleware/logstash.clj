(ns ring.middleware.logstash
  (:import [java.net Socket InetSocketAddress]
           [java.io DataOutputStream])
  (:require [clojure.data.json :as json]
            [clojure.core.async :as async]))

;; todo log with json_event format
;; todo log with tags: 2xx 3xx 4xx 5xx exception

(defn log-event [log-socket event]
  (.writeUTF 
   log-socket
   (-> event
       json/write-str
       (str \newline))))

(defn make-event-chan [host port]
  (let [chan (async/chan
              (async/sliding-buffer 10000))
        log-socket (doto (Socket.)
                     (.setKeepAlive true)
                     (.connect (InetSocketAddress. host port)
                               60000))]
    (async/go-loop [event (async/<! chan)]
      (log-event log-socket event)
      (recur (async/<! chan)))
    chan))
    
(defn wrap-logstash [handler & {:keys [host port name]
                                :or {:host "localhost"
                                     :port 4444
                                     :name "none"}}]
  (let [hostname (.getHostName (java.net.InetAddress/getLocalHost))
        events (make-event-chan host port)]
    (fn [request]
      (let [before-time (System/nanoTime)]
        (try
          (let [response (handler request)
                after-time (System/nanoTime)]
            (async/>!! 
             events
             {:type :response
              :time before-time
              :request request
              :response {:status (:status response)
                         :gen-time (- after-time before-time)}
              :app name
              :machine hostname}))
          (catch 
              Exception 
              e 
            (async/>!! events {:type :exception
                               :request request
                               :time before-time
                               :app name
                               :machine hostname
                               :exception e})))))))
                               
                             
