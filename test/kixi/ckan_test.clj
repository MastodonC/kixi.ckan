(ns kixi.ckan.ckan-test
  (:require [clojure.test :refer :all]
            [kixi.ckan :refer :all]))

(deftest get-url-test
  (testing "Testing url building"
    (is (= "http://foo.com/api/3/action/search?resource=1"
           (get-url {:ckan-client-session {:site "http://foo.com/api/3/action/"}} "search" "?resource=" "1")))
    (is (= "http://foo.com/api/3/action/foo"
           (get-url {:ckan-client-session {:site "http://foo.com/api/3/action/"}} "foo")))))
