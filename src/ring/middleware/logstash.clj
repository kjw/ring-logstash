(ns ring.middleware.logstash
  (:import [java.net Socket InetSocketAddress]
           [java.io DataOutputStream])
  (:require [clojure.data.json :as json]))

;; todo log exceptions
;; todo log with json_event format
;; todo log with tags: 2xx 3xx 4xx 5xx exception

(def hostname (.getHostName (java.net.InetAddress/getLocalHost)))

(defn wrap-logstash [handler & {:keys [host port name]
                                :or {:host "localhost"
                                     :port 4444
                                     :name ""}}]
  (let [log-socket (doto (Socket.)
                     (.setKeepAlive true)
                     (.connect (InetSocketAddress. host port)
                               60000))]
    (fn [request]
      (let [before-time (System/nanoTime)
            response (handler request)
            after-time (System/nanoTime)]
        (.writeUTF
         log-socket
         (-> request
             (merge 
              {:status (:status response)
               :app name
               :machine hostname
               :response-gen-time (- after-time before-time)})
             (json/write-str)
             (str \newline)))))))
        
