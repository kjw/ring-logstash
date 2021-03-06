(ns ring.middleware.logstash
  (:import [java.net Socket InetSocketAddress InetAddress]
           [java.io PrintStream])
  (:require [clojure.data.json :as json]
            [clojure.core.async :as async]
            [clj-time.core :as dt]
            [clj-time.format :as df]))

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

(defn wait-till-connection [socket]
  (future
    (try
      (loop []
          (if (.isConnected socket)
            true
            (do
              (Thread/sleep 500)
              (recur))))
      (catch Exception e false))))

(defn make-socket [addr]
  (try
    (let [socket (doto (Socket.)
                   (.setKeepAlive true)
                   (.connect addr 0))]
      (when (not @(wait-till-connection socket))
        (throw (Exception.)))
      socket)
    (catch Exception e {})))

(defn log-event* [log-addr log-context event]
  (try
    (let [log-socket (:socket log-context)]
      (when (or (nil? log-socket) 
                (.isClosed log-socket))
        (throw (Exception.)))
      (let [ps (or (:writer log-context)
                   (PrintStream. (.getOutputStream log-socket)))]
        (.print
         ps
         (-> event
             apply-tags
             json/write-str
             (str \newline)))
        {:socket log-socket
         :writer ps}))
    (catch Exception e {})))

(defn log-event [log-addr log-context event]
  (let [result (log-event* log-addr log-context event)]
    (if-not (nil? (:socket result))
      result
      (recur log-addr 
             {:socket (make-socket log-addr)}
             event))))

(defn make-event-chan [host port]
  (let [chan (async/chan
              (async/sliding-buffer 10000))
        log-addr (InetSocketAddress.
                  (InetAddress/getByName host)
                  port)]
    (async/go-loop [event (async/<! chan)
                    log-context nil]
      (let [possibly-new-context (log-event log-addr log-context event)]
        (recur (async/<! chan) possibly-new-context)))
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
                     :response-headers (:headers response)
                     :response-status (:status response)
                     :response-gen-time (float (/ (- after-time before-time) 
                                                  1000000))
                     "@timestamp" ts
                     "@version" "1"}))))
            response)
          (catch
              Exception 
              e 
            (async/go
              (async/>! events (-> request
                                   clean-request
                                   (merge
                                    {"type" :exception
                                     "message" (:uri request)
                                     "source" name
                                     "source-host" hostname
                                     :exception {:name (.toString e)
                                                 :message (.getMessage e)
                                                 :trace (map #(.toString %) (.getStackTrace e))}
                                     "@timestamp" ts
                                     "@version" "1"}))))
            (throw e)))))))
                               
                             
