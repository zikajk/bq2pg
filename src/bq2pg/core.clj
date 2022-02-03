(ns bq2pg.core
  (:require
   [bq2pg.bq     :refer [create-bq-client]]
   [bq2pg.config :refer [CONFIG load-config!]]
   [bq2pg.gcs    :refer [create-gcs-client]]
   [bq2pg.integration :refer [integrate!]]
   [bq2pg.proxy  :refer [set-proxy!]])
  (:gen-class))

(defn -main [& args]
  (load-config! {:filepath (first args)})
  (let [gcs-client (create-gcs-client CONFIG)
        bq-client (create-bq-client CONFIG)]
    (set-proxy! CONFIG)
    (integrate! gcs-client bq-client CONFIG)))
