(ns kixi.ckan
  "Component that maintains communication with CKAN."
  (:require [slingshot.slingshot :refer [throw+ try+]]
            [clj-http.client :as client]
            [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log]
            [cheshire.core :as json]
            [kixi.ckan.data :as data]))

(defprotocol ClientSession
  (-package-list [this])
  (-package-show [this id])
  (-package-new [this dataset])
  (-resource-new [this package_id resource-metadata])
  (-datastore-search [this id])
  (-datastore-upsert [this id data])
  (-datastore-insert [this package_id data]))

(defrecord CkanClientSession [opts]
  component/Lifecycle
  (start [this]
    (assoc this :ckan-client-session {:site (:site opts)
                                      :api-key (:api-key opts)}))
  (stop [this]
    (dissoc this :ckan-client-session))

  ClientSession
  (-package-list [this]
    (let [url (str "http://" (-> this :ckan-client-session :site) "package_list")]
      (try
        (let [result (-> (client/get url
                                     {:content-type :json
                                      :accept :json})
                         :body
                         (json/parse-string true)
                         :result)]
          result)
        (catch Throwable t
          (log/errorf t "Could not get the names of the site's datasets")
          (throw t)))))

  (-package-show [this id]
    (let [url (str "http://" (-> this :ckan-client-session :site) "package_show?id="id)]
      (try+
        (let [result (-> (client/get url
                                     {:content-type :json
                                      :accept :json})
                         :body
                         (data/unparse))]
          result)
        (catch [:status 404] {:keys [request-time headers body]}
          (log/warnf "Could not find a package with id: %s" id))
        (catch Throwable t
          (log/errorf t "Failed to get contents of a package.")
          (throw t)))))

  (-package-new [this dataset]
    (let [url     (str "http://" (-> this :ckan-client-session :site) "package_create")
          api-key (-> this :ckan-client-session :api-key)]
      (try
        (let [response (-> (client/post url
                                        {:body dataset
                                         :headers {"Authorization" api-key}
                                         :content-type :json
                                         :accept :json})
                           :body
                           (json/parse-string true))
              success? (:success response)]
          (when success?
            (let [id (-> response :result :id)]
              (log/infof "Package has been created successfully. ID: %s" id)
              {:id id})))
        (catch Throwable t
          (log/errorf t "Failed to create a package.")
          (throw t)))))

  (-resource-new [this package_id resource-metadata]
    (let [url (str "http://" (-> this :ckan-client-session :site) "resource_create")
          api-key (-> this :ckan-client-session :api-key)]
      (try
        (let [response (-> (client/post url
                                        {:body resource-metadata
                                         :headers {"Authorization" api-key}
                                         :content-type :json
                                         :accept :json})
                           :body
                           (json/parse-string true))
              success? (:success response)]
          (if success?
            (let [id (-> response :result :id)]
              (log/infof "Resource has been created successfully. ID: %s" id)
              {:id id})
            (log/errorf "Failed to create a resource. Error: %s" (:error response))))
        (catch Throwable t
          (log/errorf t "Failed to create a resource.")
          (throw t)))))

  (-datastore-search [this id]
    (let [site-url (-> this :ckan-client-session :site)]
      (try+
       (data/page-results site-url id 0)
       (catch [:status 404] {:keys [request-time headers body]}
         (log/warnf "Could not find a resource with id: %s" id))
       (catch Throwable t
         (log/errorf "Failed to search for a resource with id: %s" id)
         (throw t)))))

  (-datastore-upsert [this id data]
    (let [url      (str "https://"(-> this :ckan-client-session :site)
                        "datastore_upsert?resource_id="id)
          api-key  (-> this :ckan-client-session :api-key)]
      (try
        (let [result (-> (client/post url
                                      {:content-type :json
                                       :headers {"Authorization" api-key}
                                       :params {:resource-id id
                                                :method "update"}
                                       :body {"resource" data}
                                       :accept :json}))]
           result)
        (catch Throwable t
          (log/errorf t "Could not upsert data for resource with id: %s" id)
          (throw t)))))

  (-datastore-insert [this package_id data]
    (let [url      (str "http://"(-> this :ckan-client-session :site)
                        "datastore_create")
          api-key  (-> this :ckan-client-session :api-key)]
      (try
        (let [result (-> (client/post url
                                      {:content-type :json
                                       :headers {"Authorization" api-key}
                                       :body data
                                       :accept :json})
                         :body
                         (json/parse-string true)
                         :success)]
          result)
        (catch Throwable t
          (log/errorf t "Could not insert a new resource to package with id: %s"
                      package_id)
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

(defn resource-new
  "Create new resource belonging to a specified package."
  [session package_id resource-metadata]
  (-resource-new session package_id resource-metadata))

(defn datastore-search
  "Gets data from a table in the DataStore"
  [session id]
  (-datastore-search session id))

(defn datastore-upsert
  "Updates a resource in the DataStore with a given id."
  [session id data]
  (-datastore-upsert session id data))

(defn datastore-insert
  "Creates a new DataStore resource in a package with a given package id."
  [session package_id data]
  (-datastore-insert session package_id data))
