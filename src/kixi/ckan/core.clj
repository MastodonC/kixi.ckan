(ns kixi.ckan.core
  (:require [clj-http.client :as client]
            [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log]
            [clojure.data.json :as json]
            [kixi.ckan.data :as data]))

(defprotocol ClientSession
  (-package-list [this])
  (-package-show [this id])
  (-package-new [this dataset])
  (-datastore-search [this id])
  (-datastore-upsert [this id data]))

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
                         (data/unparse))]
          result)
        (catch Throwable t
          (log/errorf t "Could not get the metadata of the dataset with id %s" id)
          (throw t)))))
  (-package-new [this dataset]
    (let [url     (str (-> this :ckan-client-session :site) "package_create")
          api-key (:api-key this)]
      (log/infof "Attempting to create a new dataset.")
      (try
        (client/post url
                     {:body "{\"json\": \"input\"}"
                      :headers {"Authorization" api-key}
                      :content-type :json
                      :accept :json})
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
                         (data/unparse))]
          result)
        (catch Throwable t
          (log/errorf t "Could not get data from the datastore table with id: %s" id)
          (throw t)))))
  (-datastore-upsert [this id data]
    (let [url      (str (-> this :ckan-client-session :site)
                        "datastore_upsert?resource_id="id)
          api-key  (:api-key this)]
      (try
        (let [result (-> (client/post url
                                      {:content-type :json
                                       :headers {"Authorization" api-key}
                                       :params {:resource-id id
                                                :method "update"}
                                       :body (json/write-str data)
                                       :accept :json}))]
          result)
        (catch Throwable t
          (log/errorf t "Could not upsert data for resource with id: %s" id)
          (throw t))))))

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
  "Gets data from a table in the DataStore"
  [session id]
  (-datastore-search session id))

(defn datastore-upsert
  "Updates or inserts into a table in the DataStore"
  [session id data]
  (-datastore-upsert session id data))
