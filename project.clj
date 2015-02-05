(defproject kixi/ckan "0.1.4-SNAPSHOT"
  :description "Clojure client for the CKAN API."
  :url "https://github.com/MastodonC/kixi.ckan"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :source-paths ["src"]

  :dependencies [[org.clojure/clojure        "1.6.0"]

                 ;; http client to communicate with ckan
                 [clj-http                   "1.0.1"]
                 ;; Process clj-http Slingshot Stones
                 [slingshot                  "0.12.1"]

                 ;; component
                 [juxt/modular "0.2.0"]

                 ;; data
                 [cheshire                   "5.4.0"]

                 ;; logging
                 [org.clojure/tools.logging  "0.3.0"]]

  :repositories [["releases" {:url "https://clojars.org/repo" :username :env/clojars_username :password :env/clojars_password}]]

  :uberjar-name "kixi.ckan.jar")
