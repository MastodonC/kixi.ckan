(ns kixi.ckan.data
  "Functions to parse, unparse and transform data."
  (:require [slingshot.slingshot   :refer [throw+ try+]]
            [cheshire.core         :as json]
            [clojure.edn           :as edn]
            [clj-http.client       :as client]
            [clojure.tools.logging :as log]))

(defn fix-map-key
  "Takes string keys and format them before they
  get turned into keywords."
  [keys-string]
  (-> keys-string
      (clojure.string/lower-case)
      (clojure.string/replace #"[ \n]" "_")
      (clojure.string/replace #"[^A-Za-z0-9-_]" "")))

(defn unparse
  "Takes DataStore data represented as JSON and turns it to clojure data structure."
  [data]
  (let [data-edn (-> data
                     (json/parse-string
                      (fn [k] (keyword (fix-map-key k))))
                     (get :result))]
    (->> (apply concat data-edn)
         (apply hash-map))))

(defn fields->lookup-fields
  "Takes a vector of maps, each containing a type and id, and converts them
  to a hash-map where key is the field id and value is a type."
  [fields]
  (apply merge (map (fn [field]
                  (hash-map (get field "id")
                            (get field "type"))) fields)))

(defn prepare-resource-for-insert
  "Transform and prepare data for inserting into DataStore."
  [package_id new-resource_id data]
  (let [transformed (assoc data "package_id" package_id
                           "resource_id" new-resource_id
                           "method" "insert"
                           "force" true)]
    (json/encode transformed)))

(defn page-results
  "Retrieve all pages of records of a given resource as a lazy sequence.
  Uses the default offset of 100 that is used by CKAN."
  [site-url resource_id offset]
  (let [url       (str "http://" site-url "datastore_search?offset=" offset "&resource_id=" resource_id)
        result    (client/get url {:content-type :json :accept :json})
        unparsed  (-> result :body unparse)
        total     (get unparsed :total)
        next-page (+ offset 100)]
     (lazy-cat
      (get unparsed :records)
      (when (< next-page total)
        (page-results site-url resource_id next-page)))))
