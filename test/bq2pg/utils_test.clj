(ns bq2pg.utils-test
  (:require [clojure.test :refer [deftest testing is]]
            [bq2pg.utils :refer [gcs-directory load-local-state
                                 read-last-state
                                 rand-str
                                 before?
                                 interval>]]
            [clj-time.core :as t]))

(deftest gcs-directory-test
  (testing "creating gcs-directory path"
    (is
     (= "gs://testing-bucket/path/to/file"
        (gcs-directory "testing-bucket" "path" "to" "file")))))

(deftest load-local-state-test
  (testing "loading local state file"
    (is (= clojure.lang.PersistentArrayMap
           (type (load-local-state))))))

(deftest read-local-state-test
  (testing "reading a value from local state file"
    (is (nil? (read-last-state :non-existing-key)))))

(deftest rand-str-test
  (testing "generating random string of len 12"
    (let [random-string (rand-str 12)]
      (is (and (string? random-string)
               (= 12 (count random-string)))))))

(deftest before-test
  (testing "date before"
    (let [date (t/now)]
      (is (= false (before? date date))))))

(deftest interval>-test
  (testing "bigger interval"
    (is (true? (interval> 1000 2100 1))
        (false? (interval> 1000 2000 1)))))
