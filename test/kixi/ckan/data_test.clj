(ns kixi.ckan.data-test
  (:require [clojure.test :refer :all]
            [kixi.ckan.data :refer :all]))

(deftest fix-map-key-test
  (testing "The map keys are formatted correctly.")
  (is (= "level_description"
         (fix-map-key "Level description")))
  (is (= "_id"
         (fix-map-key "_id")))
  (is (= "period_of_coverage"
         (fix-map-key "Period of coverage")))
  (is (= "notes_denominator_indicator_value"
         (fix-map-key "Notes
Denominator, indicator value")))
  (is (= "indicator_value_rate"
         (fix-map-key "Indicator value (rate)"))))

(deftest unparse-test
  (is (= {:fields [{:type "numeric", :id "foo"} {:type "numeric", :id "bar"}],
          :records [{:foo "1.3"} {:bar "1.3"}],
          :total 2,
          :_links {:start "/api/3/action/datastore_search?resource_id=1",
                   :next "/api/3/action/datastore_search?offset=100&resource_id=1"}, :resource_id "1"}
         (unparse "{\"help\" : \"help\",
                   \"success\": true,
                   \"result\" : {\"total\" : 2,
                               \"resource_id\" : \"1\",
                               \"fields\" : [{\"type\" : \"numeric\", \"id\" : \"foo\"},
                                            {\"type\" : \"numeric\", \"id\" : \"bar\"}],
                               \"records\" : [{\"foo\" : \"1.3\"},
                                          {\"bar\" : \"1.3\"}],
                               \"_links\": {\"start\" : \"/api/3/action/datastore_search?resource_id=1\",
                                          \"next\" : \"/api/3/action/datastore_search?offset=100&resource_id=1\"}}}"))))

(deftest fields->lookup-fields-test
  (is (= {"foo" "numeric" "bar" "string"}
         (fields->lookup-fields [{"type" "numeric" "id" "foo"}
                                 {"type" "string" "id" "bar"}]))))
