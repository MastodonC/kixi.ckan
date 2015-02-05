(ns kixi.ckan
  "Component that maintains communication with CKAN."
  (:require [slingshot.slingshot :refer [throw+ try+]]
            [clj-http.client :as client]
            [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log]
            [cheshire.core :as json]
            [kixi.ckan.data :as data]))

(defn get-url
  "Build url string by joining site url stored in config with
  api method and params."
  [ckan-client & api-method]
  (apply str (-> ckan-client :ckan-client-session :site) api-method))

(defprotocol ClientSession
  (-package-list [this])
  (-package-list-with-resources [this])
  (-package-show [this id])
  (-package-create [this dataset])

  (-resource-show [this id])
  (-resource-create [this package_id resource-metadata])
  (-resource-delete [this resource_id])

  (-datastore-search [this id])
  (-datastore-upsert [this id data])
  (-datastore-insert [this package_id data])

  (-organization-show [this id])

  (-tag-show [this id]))

(defrecord CkanClientSession [opts]
  component/Lifecycle
  (start [this]
    (assoc this :ckan-client-session {:site (:site opts)
                                      :api-key (:api-key opts)}))
  (stop [this]
    (dissoc this :ckan-client-session))

  ClientSession

  (-package-list [this]
    (let [url (get-url this "package_list")]
      (try
        (-> (client/get url
                        {:content-type :json
                         :accept :json})
            :body
            (json/parse-string true))
        (catch Throwable t
          (log/errorf t "Could not get the names of the site's datasets")
          (throw t)))))

  (-package-list-with-resources [this]
    (let [url (get-url this "current_package_list_with_resources")]
      (try
        (-> (client/get url
                        {:content-type :json
                         :accept :json})
            :body
            (json/parse-string true))
        (catch Throwable t
          (log/errorf t "Could not get the current package list with resources.")
          (throw t)))))

  (-package-show [this id]
    (let [url (get-url this "package_show?id=" id)]
      (try+
        (-> (client/get url
                        {:content-type :json
                         :accept :json})
            :body
            (json/parse-string true))
        (catch [:status 404] {:keys [request-time headers body]}
          (log/warnf "Could not find a package with id: %s" id))
        (catch Throwable t
          (log/errorf t "Failed to get contents of a package.")
          (throw t)))))

  (-package-create [this dataset]
    (let [url     (get-url this "package_create")
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

  (-resource-show [this resource_id]
    (let [url (get-url this "resource_show?id=" resource_id)]
      (try+
        (let [result (-> (client/get url
                                     {:content-type :json
                                      :accept :json})
                         :body
                         (json/parse-string true))]
          result)
        (catch [:status 404] {:keys [request-time headers body]}
          (log/warnf "Could not find a resource with id: %s" resource_id))
        (catch Throwable t
          (log/errorf t "Failed to get metadata of a resource %s." resource_id)
          (throw t)))))

  (-resource-create [this package_id resource-metadata]
    (let [url     (get-url this "resource_create")
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
          (when success?
            (let [id (-> response :result :id)]
              (log/infof "Resource has been created successfully. ID: %s" id)
              {:id id})))
        (catch Throwable t
          (log/error t "Failed to create a resource.")
          (throw t)))))

  (-resource-delete [this resource_id]
    (let [url     (get-url this "resource_delete?id=" resource_id)
          api-key (-> this :ckan-client-session :api-key)]
      (try
        (let [response (-> (client/post url
                                        {:headers {"Authorization" api-key}
                                         :content-type :json
                                         :accept :json})
                           :body
                           (json/parse-string true))
              success? (:success response)]
          (if success?
            true
            (log/errorf "Failed to delete a resource. Error: %s" (:error response))))
        (catch Throwable t
          (log/errorf t "Failed to delete a resource.")
          (throw t)))))

  (-datastore-search [this id]
    (let [site-url (get-url this)]
      (try+
       (data/page-results site-url id 0)
       (catch [:status 404] {:keys [request-time headers body]}
         (log/warnf "Could not find a resource with id: %s" id))
       (catch Throwable t
         (log/errorf "Failed to search for a resource with id: %s" id)
         (throw t)))))

  (-datastore-upsert [this id data]
    (let [url      (get-url this "datastore_upsert?resource_id=" id)
          api-key  (-> this :ckan-client-session :api-key)]
      (try
        (-> (client/post url
                         {:content-type :json
                          :headers {"Authorization" api-key}
                          :body data
                          :accept :json})
            :body
            (json/parse-string true))
        (catch Throwable t
          (log/errorf t "Could not upsert data for resource with id: %s" id)
          (throw t)))))

  (-datastore-insert [this package_id data]
    (let [url      (get-url this "datastore_create")
          api-key  (-> this :ckan-client-session :api-key)]
      (try
        (-> (client/post url
                         {:content-type :json
                          :headers {"Authorization" api-key}
                          :body data
                          :accept :json})
            :body
            (json/parse-string true))
        (catch Throwable t
          (log/errorf t "Could not insert a new resource to package with id: %s"
                      package_id)
          (throw t)))))

  (-organization-show [this id]
    (let [url (get-url this "organization_show?id=" id)]
      (try+
        (-> (client/get url
                        {:content-type :json
                         :accept :json})
            :body
            (json/parse-string true))
        (catch [:status 404] {:keys [request-time headers body]}
          (log/warnf "Could not find an organization with id: %s" id))
        (catch Throwable t
          (log/errorf t "Failed to get details of an organization %s." id)
          (throw t)))))

  (-tag-show [this id]
    (let [url (get-url this "tag_show?id=" id)]
      (try+
        (-> (client/get url
                        {:content-type :json
                         :accept :json})
            :body
            (json/parse-string true))
        (catch [:status 404] {:keys [request-time headers body]}
          (log/warnf "Could not find an organization with id: %s" id))
        (catch Throwable t
          (log/errorf t "Failed to get details of an organization %s." id)
          (throw t))))))

(defn new-ckan-client-session [opts]
  (->CkanClientSession opts))

(defn package-list
  "Return a list of the names of the site’s datasets (packages)."
  [session]
  (-package-list session))

(defn package-list-with-resources
  "Return a list of the site’s datasets (packages) and their resources.
  The list is sorted most-recently-modified first."
  [session]
  (-package-list-with-resources session))

(defn package-show
  "Return the metadata of a dataset (package) and its resources."
  [session id]
  (-package-show session id))

(defn package-create
  "Create a new dataset (package)."
  [session dataset]
  (-package-create session dataset))

(defn resource-show
  "Return the metadata of a resource."
  [session resource_id]
  (-resource-show session resource_id))

(defn resource-create
  "Appends a new resource to a datasets list of resources."
  [session package_id resource-metadata]
  (-resource-create session package_id resource-metadata))

(defn resource-delete
  "Delete a resource from a dataset.
  You must be a sysadmin or the owner of the resource to delete it."
  [session resource_id]
  (-resource-delete session resource_id))

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

(defn organization-show
  "Return the details of a organization for a given id or name. Includes datasets."
  [session id]
  (-organization-show session id))

(defn tag-show
  "Return the details of the tag, including a list of all of the tag’s
  datasets and their details."
  [session id]
  (-tag-show session id))
