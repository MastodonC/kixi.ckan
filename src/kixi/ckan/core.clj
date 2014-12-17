(ns kixi.ckan.core
  (:require [clj-http.client :as client]
            [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log]
            [clojure.data.json :as json]))

;; url to be stored in session: http://<site_name>.ckan.org/api/3/action/

(defprotocol ClientSession
  (-package-list [this]))

(defrecord CkanClientSession [opts]
  component/Lifecycle
  (start [this]
    ;; TODO authentication
    (assoc this :ckan-client-session {:site (:site opts)}))
  (stop [this]
    (dissoc this :ckan-client-session))
  ClientSession
  (-package-list [this]
    (let [url (str (-> this :ckan-client-session :site) "package_list")]
      (log/infof "Attempting to retrieve package list from url: %s" url)
      (try
        (let [result (-> (client/get url
                                     {:content-type :json
                                      :accept :json})
                         :body
                         (json/read-str :key-fn keyword)
                         :result)]
          result)
        (catch Throwable t
          (log/errorf t "Could not get the names of the site's datasets")
          (throw t))))))

(defn new-ckan-client-session [opts]
  (->CkanClientSession opts))

(defn package-list
  "Return a list of the names of the site’s datasets (packages)."
  [session]
  (-package-list session))
