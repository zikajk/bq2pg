(ns bq2pg.proxy-test
  (:require [clojure.test :refer [deftest testing is]]
            [bq2pg.proxy :refer [get-credentials]]))

(deftest get-credentials-test
  (testing "get username and password for proxy"
    (let [proxy-cfg {:proxy-host "host"
                     :proxy-user "user"
                     :proxy-password "password"}
          options   {:host "host"}]
      (is (= (get-credentials proxy-cfg options)
             ["user" "password"])))))
