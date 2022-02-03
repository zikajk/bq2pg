(ns user
  (:require [bq2pg.config :refer [load-config-repl! CONFIG]]
            [bq2pg.integration :refer [integrate!]]
            [bq2pg.bq :refer [create-bq-client]]
            [bq2pg.gcs :refer [create-gcs-client]]
            [malli.clj-kondo :as mc]
            [malli.dev :as md]))

(-> (mc/collect *ns*) (mc/linter-config))
(md/start!)

(load-config-repl! {:filepath "./example/pokemon_integration.edn"
                     :extra {:export? true
                             :import? true
                             :gcs-name "unknown"
                             :gcs-folder "bq2pg"
                             :sa-path "/users/akiz/.config/gcp-batch-runner-dev-sa.json"
                             :db {:dbtype "postgres"
                                  :dbname "postgres"
                                  :user "postgres"
                                  :host "localhost"
                                  :port 5432
                                  :password "my_password"}}})

(def gcs-client (create-gcs-client CONFIG))
(def bq-client (create-bq-client CONFIG))

(defn integrate-dev! []
  (integrate! gcs-client bq-client CONFIG))
