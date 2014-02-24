(ns ring.middleware.logstash
  (:import [java.net Socket InetSocketAddress]
           [java.io DataOutputStream])
  (:require [clojure.data.json :as json]
            [clojure.core.async :as async]
            [clj-time.core :as dt]
            [clj-time.format :as df]))

;; todo switch to edn_lines codec?

;; logstash config example
;;
;; input {
;;   tcp {
;;     codec => "json_lines",
;;     port => 4444
;;   }
;; }

(defn apply-tags [event]
  (let [status (get-in event ["@fields" :response :status])
        status-tag (cond
                    (nil? status)
                    :exception
                    (>= 199 status 100)
                    :informational
                    (>= 299 status 200)
                    :success
                    (>= 399 status 300)
                    :redirection
                    (>= 499 status 400)
                    :client-error
                    (>= 599 status 500)
                    :server-error)]
    (assoc event "@tags" [status-tag])))

(defn log-event [log-socket event]
  (.writeUTF 
   log-socket
   (-> event
       apply-tags
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
                                     :app "none"}}]
  (let [hostname (.getHostName (java.net.InetAddress/getLocalHost))
        events (make-event-chan host port)
        timestamp-fmt (df/formatters :date-hour-minute-second-fraction)]
    (fn [request]
      (let [ts (df/unparse timestamp-fmt (dt/now))]
        (try
          (let [before-time (System/nanoTime)
                response (handler request)
                after-time (System/nanoTime)]
            (async/>!!
             events
             {"@type" :response
              "@fields"
              {:app name
               :machine hostname
               :request request
               :response {:status (:status response)
                         :gen-time (/ (- after-time before-time)
                                      1000000)}}
              "@timestamp" ts}))
          (catch
              Exception 
              e 
            (async/>!! events {"@type" :exception
                               "@fields"
                               {:app name
                                :machine hostname
                                :exception e
                                :request request}
                               "@timestamp" ts})))))))
                               
                             
