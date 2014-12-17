(ns kixi.ckan.main
  (:gen-class)
  (:require [com.stuartsierra.component :as component]))

(defrecord ReplServer [config]
  component/Lifecycle
  (start [this]
    (println "Starting REPL server " config)
    (assoc this :repl-server
           (apply nrepl-server/start-server :handler cider-nrepl-handler (flatten (seq config)))))
  (stop [this]
    (println "Stopping REPL server with " config)
    (nrepl-server/stop-server (:repl-server this))
    (dissoc this :repl-server)))

(defn mk-repl-server [config]
  (ReplServer. config))

(defn build-application [system opts]
  (-> system
      (cond-> (:repl opts)
              (assoc :repl-server (mk-repl-server {:port (:repl-port opts)})))))

(defn -main [& args]
  (println "Starting kixi.eventlog")
  (let [[opts args banner]
        (cli args
             ["-h" "--help" "Show help"
              :flag true :default false]
             ["-R" "--repl" "Start a REPL"
              :flag true :default true]
             ["-r" "--repl-port" "REPL server listen port"
              :default 4001 :parse-fn #(Integer. %)])]
    (when (:help opts)
      (println banner)
      (System/exit 0))
    (alter-var-root #'kixi/instance (fn [_]
                                      (component/start
                                       (build-application
                                        (kixi/new-system)
                                        opts))))))
