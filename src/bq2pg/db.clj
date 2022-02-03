(ns bq2pg.db
  (:require
   [clojure.string :as str]
   [clojure.java.io :as io]
   [next.jdbc :as jdbc]
   [next.jdbc.date-time]
   [clojure.data.csv :as csv]
   [bq2pg.config :refer [Table-name]]
   [malli.core :as m]
   [taoensso.timbre :as timbre]
   [next.jdbc.types
    :as types]))

(defn connectable?
  "Returns true if `conn` can be used by JDBC to initialize connection
  otherwise returns false.
  `Conn` can be map or 'org.postgresql.jdbc.PgConnection'"
  [conn]
  (or (map? conn)
      (= (type conn)
         org.postgresql.jdbc.PgConnection)))

(def Connectable [:fn connectable?])

(defn- get-type
  "Gets Postgresql type for selected `column-name` in `table` with jdbc based on `connectable`."
  [connectable table-name column-name]
  (let [query "SELECT data_type FROM information_schema.columns where table_schema = '%s'and table_name = '%s' and upper(column_name) = '%s'"
        [schema table] (str/split (name table-name) #"\.")
        col-name (str/upper-case column-name)]
    (:columns/data_type (jdbc/execute-one!
                         connectable
                         [(format query schema table col-name)]))))

(m/=> get-type [:=>
                [:cat Connectable Table-name string?]
                [:enum "bool" "bit" "int8" "bigserial" "oid" "bytea" "char" "bpchar" "numeric" "int4" "serial" "int2" "smallserial"
                 "float4" "float8" "money" "name" "text" "character varying" "varchar" "date" "time" "timetz" "timestamp" "timestampz"]])

(defn convert-type
  "Convert Postgresql `type` to conversion function"
  [type]
  (case type
    ("bool" "bit")                                   #(types/as-bit %)
    ("int8" "bigserial" "oid")                       #(types/as-bigint %)
    "bytea"                                          #(types/as-binary %)
    ("char" "bpchar")                                #(types/as-char %)
    "numeric"                                        #(types/as-numeric %)
    ("int4" "serial")                                #(types/as-integer %)
    ("int2" "smallserial")                           #(types/as-smallint %)
    "float4"                                         #(types/as-real %)
    ("float8" "money")                               #(types/as-double)
    ("name", "text", "character varying", "varchar") #(types/as-varchar %)
    "date"                                           #(try (.parse (java.text.SimpleDateFormat. "yyyy-MM-dd") %)
                                                           (catch Exception _ nil))
    ("time", "timetz")                               #(try (types/as-time %)
                                                           (catch Exception _ nil))
    ("timestamp", "timestampz")                      #(try (types/as-timestamp %)
                                                           (catch Exception _ nil))
    #(types/as-other %)))

(m/=> convert-type [:=>
                    [:cat [:enum "bool" "bit" "int8" "bigserial" "oid" "bytea" "char" "bpchar" "numeric" "int4" "serial" "int2" "smallserial"
                       "float4" "float8" "money" "name" "text" "character varying" "varchar" "date" "time" "timetz" "timestamp" "timestampz"]]
                    fn?])

(def Header [:vector string?])

(defn- get-conv-fns
  "Takes `header` and maps every column in `table` to matched conversion functions."
  [connectable table-name header]
  (mapv #(convert-type (get-type connectable table-name %)) header))

(m/=> get-conv-fns [:=>
                    [:cat Connectable Table-name Header]
                    vector?])

(defn- build-query
  "Takes `table` and `header` and build sql query template for next.jdbc"
  [table header]
  (let [cols (str/join "," (map #(format "\"%s\"" %) header))
        placeholders (str/join "," (for [_ header] "?"))
        query (format "insert into %s (%s) values (%s)" (name table) cols placeholders)]
    query))

(m/=> build-query [:=>
                   [:cat Table-name Header]
                   string?])

(defn- get-header
  "Takes first row from `coll` as a header
   which is splitted on a `delimiter`. "
  [coll delimiter]
  (->> (str/split (first coll)
                  (re-pattern delimiter))
       (mapv str)))

(m/=> get-header [:=>
                  [:cat [:sequential string?] string?]
                  Header])

(defn truncate-table!
  "Tries to reuse or create jdbc connection based on `connectable`  and truncate `table`.
   `Connectable` can be map or `org.postgresql.jdbc.PgConnection.
   `Table` is a string in 'schema.table' form."
  [connectable table-name]
  (try
    (jdbc/execute! connectable [(format "truncate table %s;" (name table-name))])
    (timbre/info (format "Table: %s truncated!" table-name))
    (catch Exception _
      (timbre/warn (format "Table: %s can't be truncated!" table-name))
      )))

(m/=> truncate-table! [:=>
                       [:cat Connectable Table-name]
                       any?])

(defn table-exists?
  "Checks if `table` exists in db with jdbc based on `connectable`."
  [connectable table-name]
  (let [[schema table] (str/split table-name #"\.")
        query (format "select tablename from pg_tables where schemaname='%s' and tablename='%s';" schema table)]
    (-> (jdbc/execute! connectable [query])
        seq)))

(m/=> table-exists? [:=>
                     [:cat Connectable Table-name]
                     [:or seq? nil?]])

(defn create-table! [con table-name table-header]
  (let [header (str/join " text," (map #(format "\"%s\"" %) table-header))

        query (format "CREATE TABLE %s (%s text);" table-name header)]
    (timbre/info query )
    (jdbc/execute! con [query])))

(defn insert-csv-from-stdin!
  "Inserts csv from `stdin` parsed with `delimiter` to `table` via next.jdbc `con`."
  [stdin con table-name delimiter]
  (with-open [reader (io/reader stdin)]
    (let [lines (line-seq reader)
          table-header (get-header lines delimiter)]
      (when-not (table-exists? con table-name)
        (create-table! con table-name table-header))
      (doall
       (let [query (build-query table-name table-header)
             conv-fns (get-conv-fns con table-name table-header)]
         (doseq [batch (partition-all 100000 (rest lines))]
           (try
             (jdbc/execute-batch! con query
                                  (->> batch
                                       (mapcat csv/read-csv)
                                       (mapv (fn [row] (map #(%1 %2) conv-fns row))))
                                  {:reWriteBatchedInserts true})
             (catch Exception e
               (timbre/debug e))
             )))))))

(m/=> insert-csv-from-stdin! [:=>
                              [:cat any? Connectable Table-name string?]
                              [:or seq? nil?]])
