(ns ring.middleware.logstash
  (:import [java.net Socket InetSocketAddress InetAddress]
           [java.io PrintStream])
  (:require [clojure.data.json :as json]
            [clojure.core.async :as async]
            [clj-time.core :as dt]
            [clj-time.format :as df]))

;; todo switch to edn_lines codec?
;; todo handle reconnecting the socket

;; logstash config example
;;
;; input {
;;   tcp {
;;     codec => json_lines
;;     port => 4444
;;   }
;; }

(defn apply-tags [event]
  (let [status (get-in event [:response-status])
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
    (assoc event "tags" [status-tag])))

(defn log-event [log-socket-out event]
  (.print
   log-socket-out
   (-> event
       apply-tags
       json/write-str
       (str \newline))))

(defn make-event-chan [host port]
  (let [chan (async/chan
              (async/sliding-buffer 10000))
        log-socket (doto (Socket.)
                     (.setKeepAlive true)
                     (.connect (InetSocketAddress.
                                (InetAddress/getByName host)
                                port)
                               60000))
        log-socket-out (PrintStream. (.getOutputStream log-socket))]
    (async/go-loop [event (async/<! chan)]
      (log-event log-socket-out event)
      (recur (async/<! chan)))
    chan))

(defn clean-request [request]
  (-> request
      (dissoc :async-channel)
      (dissoc :body)))
    
(defn wrap-logstash [handler & {:keys [host port name]
                                :or {:host "localhost"
                                     :port 4444
                                     :name "none"}}]
  (let [hostname (.getHostName (InetAddress/getLocalHost))
        events (make-event-chan host port)
        timestamp-fmt (df/formatters :date-hour-minute-second-fraction)]
    (fn [request]
      (let [ts (df/unparse timestamp-fmt (dt/now))]
        (try
          (let [before-time (System/nanoTime)
                response (handler request)
                after-time (System/nanoTime)]
            (async/go
              (async/>!
               events
               (-> request
                   clean-request
                   (merge 
                    {"type" :response
                     "message" (:uri request)
                     "source" name
                     "source-host" hostname
                     :response-status (:status response)
                     :response-gen-time (float (/ (- after-time before-time) 1000000))
                     "@timestamp" ts
                     "@version" "1"}))))
            response)
          (catch
              Exception 
              e 
            (async/>!! events (-> request
                                  clean-request
                                  (merge
                                   {"type" :exception
                                    "message" (:uri request)
                                    "source" name
                                    "source-host" hostname
                                    :exception e
                                    "@timestamp" ts
                                    "@version" "1"})))
            (throw e)))))))
                               
                             
