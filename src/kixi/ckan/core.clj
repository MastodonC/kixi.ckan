(ns kixi.ckan.core
  (:require [clj-http.client :as client]
            [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log]
            [clojure.data.json :as json]))

(defprotocol ClientSession
  (-package-list [this])
  (-package-show [this id])
  (-package-new [this dataset])
  (-datastore-search [this id]))

(defrecord CkanClientSession [opts]
  component/Lifecycle
  (start [this]
    (assoc this :ckan-client-session {:site (:site opts)
                                      :api-key (:api-key opts)}))
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
          (throw t)))))
  (-package-show [this id]
    (let [url (str (-> this :ckan-client-session :site) "package_show?id="id)]
      (try
        (let [result (-> (client/get url
                                     {:content-type :json
                                      :accept :json})
                         :body
                         (json/read-str :key-fn keyword)
                         :result)]
          result))))
  (-package-new [this dataset]
    (let [url (str (-> this :ckan-client-session :site) "package_create")]
      (log/infof "Attempting to create a new dataset.")
      (try
        (let [api-key (:api-key this)]
          (client/post url
                       {:body "{\"json\": \"input\"}"
                        :headers {"Authorization" api-key}
                        :content-type :json
                        :accept :json}))
        (catch Throwable t
          (log/errorf t "Could not get the names of the site's datasets")
          (throw t)))))
  (-datastore-search [this id]
    (let [url (str (-> this :ckan-client-session :site)
                   "datastore_search?resource_id="id)]
      (try
        (let [result (-> (client/get url
                                     {:content-type :json
                                      :accept :json})
                         :body
                         (json/read-str :key-fn keyword)
                         :result)]
          result)))))

(defn new-ckan-client-session [opts]
  (->CkanClientSession opts))

(defn package-list
  "Return a list of the names of the site’s datasets (packages)."
  [session]
  (-package-list session))

(defn package-show
  "Get a dataset, resource or other object."
  [session id]
  (-package-show session id))

(defn package-new
  "Create a new dataset (package)."
  [session dataset]
  (-package-new session dataset))

(defn datastore-search
  "Return data using Data API"
  [session id]
  (-datastore-search session id))
