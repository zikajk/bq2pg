(ns bq2pg.config
  (:require
   [bq2pg.utils :as utils]
   [clojure.string :as string]
   [cprop.core :refer [load-config]]
   [babashka.fs :as fs]
   [malli.core :as m]
   [malli.error :as me]
   [taoensso.timbre :as timbre]))


(def Ne-string
  [:and string? [:fn not-empty]])

(def Db-conf
  [:map
   [:dbtype Ne-string]
   [:dbname Ne-string]
   [:user Ne-string]
   [:host Ne-string]
   [:port [:or int? string?]]])

(def Method
  [:enum "append" "replace"])

(def Table-name
  [:re #".*\..*"])

(def Location
  [:enum "us-central1" "us-west4" "us-west2" "northamerica-northeast1" "us-east4" "us-west1" "us-west3" "southamerica-east1" "southamerica-west1" "us-east1" "northamerica-northeast2"
   "europe-west1" "europe-north1" "europe-west3" "europe-west2" "europe-west4" "europe-central2" "europe-west6" "asia-south2" "asia-east2" "asia-southeast2" "australia-southeast2"
   "asia-south1" "asia-northeast2" "asia-northeast3" "asia-southeast1" "australia-southeast1" "asia-east1" "asia-northeast1" "EU" "US"])

(def Integration
  [:map
   [:name Ne-string]
   [:query Ne-string]
   [:location Location]
   [:target-pg-table Table-name]
   [:method Method]
   [:timeout int?]])

(def Config
  [:map
   [:gcs-name Ne-string]
   [:gcs-folder Ne-string]
   [:db Db-conf]
   [:export? boolean?]
   [:import? boolean?]
   [:integrations [:vector Integration]]
   [:sa-path {:optional true} Ne-string]])

;; todo - simplify config - no nested maps
(def CONFIG nil)

(defn- set-config!
  "Alters global variable CONFIG by provided `config`."
  [config]
  (alter-var-root
   (var CONFIG)
   (constantly config)))


(defn- exit
  "Exits with provided `code`,`template` and `& args`."
  [code template & args]
  (let [out (if (zero? code) *out* *err*)]
    (binding [*out* out]
      (println (apply format template args))))
  (System/exit code))


(defn coerce-config
  "Checks and returns provided `config`."
  [config]
  (cond
    (m/validate Config config)
    config
    :else (let [report (m/explain Config config)]
            (exit 1 "%s" (me/humanize report)))))


(m/=> coerce-config [:=>
                     [:cat Config]
                     Config])


(defn parse-pgpass-pwd
  "Retuns pgpass password if there exist same host, port, db and user config. "
  [s host port db user]
  (let [[chost cport cdb cuser cpwd] (string/split s #":")]
    (when (and (= host chost) (= port cport) (= db cdb) (= user cuser))
      cpwd)))

(defn load-db-pwd [host port db user]
  (if-let [dbpwd (System/getenv "PGPASSWORD")]
    dbpwd
    (if-let [pgpass (slurp
                     (fs/file
                      (System/getProperty "user.home")
                      ".pgpass"))]
      (let [creds (string/split pgpass #"\n")]
        (some #(parse-pgpass-pwd % host port db user) creds))
      (timbre/warn "DB pwd not found in ~/.pgpass or $PGPASSWORD env var."))))

(defn load-env
  "Loads various environment variables and removes any nil values. "
  []
  (utils/remove-empty-vals
   {:proxy-host (System/getenv "PROXYHOST")
    :proxy-user (System/getenv "PROXYUSER")
    :proxy-password  (System/getenv "PROXYPASSWORD")
    :export? (Boolean/valueOf (if-let [export? (System/getenv "BQEXPORT")]
                                export? "true"))
    :import? (Boolean/valueOf (if-let [import? (System/getenv "PGIMPORT")]
                                import? "true"))
    :gcs-name (System/getenv "GCSNAME")
    :gcs-folder (System/getenv "GCSFOLDER")
    :db {:dbtype "postgres"
         :host (System/getenv "PGHOST")
         :dbname (System/getenv "PGDATABASE")
         :port (System/getenv "PGPORT")
         :user (System/getenv "PGUSER")
         :password (load-db-pwd (System/getenv "PGHOST")
                                (System/getenv "PGPORT")
                                (System/getenv "PGDATABASE")
                                (System/getenv "PGUSER"))}
    :sa-path (System/getenv "BQ2PGSA")}))


(defn- fake-exit
  "Like exit.. but without exiting REPL."
  [_ template & args]
  (let [message (apply format template args)]
    (throw (new Exception ^String message))))

(defn load-config!
  "Validates and loads CONFIG on `filepath`."
  [{filepath :filepath extra :extra}]
  (try
    (-> (load-config :file filepath :merge [(load-env) extra])
        (coerce-config)
        (set-config!))
    (catch Exception e
      (timbre/debug e)
      (timbre/error "Wrong configuration file provided!"))))

(m/=> load-config! [:=>
                    [:cat
                     [:map
                      [:filepath string?]
                      [:extra {:optional true}
                       [:map
                        [:export? {:optional true} boolean?]
                        [:import? {:optional true} boolean?]
                        [:gcs-name {:optional true} string?]
                        [:gcs-folder {:optional true} string?]
                        [:db {:optional true} Db-conf]
                        [:intregations {:optional true} [:vector Integration]]
                        [:sa-path {:optional true} Ne-string]]]]]
                    [:or Config nil?]])

(defn load-config-repl!
  "Validates and loads CONFIG on `filepath` - uses fake exit."
  [config-map]
  (with-redefs [exit fake-exit]
    (load-config! config-map)))
