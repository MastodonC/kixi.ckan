(defproject kixi.ckan "0.1.0-SNAPSHOT"
  :description "Clojure client for the CKAN API."
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :source-paths ["src"]

  :dependencies [[org.clojure/clojure        "1.6.0"]

                 ;; http
                 [clj-http                   "1.0.1"]

                 ;; component
                 [juxt/modular "0.2.0" :exclusions [ch.qos.logback/logback-classic
                                                    org.slf4j/jcl-over-slf4j
                                                    org.slf4j/jul-to-slf4j
                                                    org.slf4j/log4j-over-slf4j]]

                 ;; data
                 [org.clojure/data.json      "0.2.5"]
                 [cheshire                   "5.4.0"]

                 ;; logging
                 [org.clojure/tools.logging  "0.3.0"]]

  :uberjar-name "kixi.ckan.jar"
  :profiles {:dev {:source-paths ["dev"]}})
