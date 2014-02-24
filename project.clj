(defproject kjw/ring-logstash "0.1.0"
  :description "Send ring requests, response statuses and exceptions to logtash via TCP."
  :url "http://github.com/kjw/ring-logstash"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/data.json "0.2.4"]
                 [org.clojure/core.async "0.1.267.0-0d7780-alpha"]
                 [clj-time "0.6.0"]])
