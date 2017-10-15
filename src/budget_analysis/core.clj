(ns budget-analysis.core
  (:gen-class))

(defn reload [] (use 'budget-analysis.core :reload))

(require '[clojure.data.csv :as csv]
         '[clojure.java.io :as io]
         '[clojure.java.jdbc :as sql]
         '[clojure.set :refer [rename-keys]]
         '[clj-time.format :refer [parse-local-date formatter]])

(def db {:classname   "org.sqlite.JDBC"
         :subprotocol "sqlite"
         :subname     "test.db"})

(defn create-tables []
  "Creates all necessary tables for the functioning of the app"
  (sql/db-do-commands db 
                      (sql/create-table-ddl :transactions
                                            [:id :integer :primary :key :autoincrement]
                                            [:description "varchar(255)"]
                                            [:spendtype "varchar(255)"]
                                            [:transdate :date]
                                            [:amount "decimal(12, 2)"]
                                            [:account "varchar(255)"])))

(def num-format (java.text.NumberFormat/getInstance java.util.Locale/US))

(def parse-transforms
  {:transdate #(parse-local-date (formatter "MM/dd/yyyy") %)
   :amount #(double (.parse num-format %))})

(def deserialize-transforms
  {:transdate parse-local-date
   :amount double})

(def chase-col-map
  {"Type"        :spendtype
   "Trans Date"  :transdate
   "Description" :description
   "Amount"      :amount})

(def jp-morgan-col-map
  {"Type"         :spendtype
   "Trade Date"   :transdate
   "Description"  :description
   "Amount Local" :amount})

(defn parse-double [str] (Double/parseDouble str))

(defn evolve
  "takes a map of key to transform function and applies them to a map of keys to values"
  [transforms values]
  (reduce (fn [acc k]
            (assoc acc k ((transforms k) (values k))))
          values
          (clojure.set/intersection (into #{} (keys transforms)) 
                                    (into #{} (keys values)))))

(def identifying-transaction-props [:description :transdate :amount :account :spendtype])

(defn same-transaction? 
  "takes two transaction records and checks if they are the same
  based on the set of their properties denoted in 'keys'"
  [a b]
  (= (select-keys a identifying-transaction-props)
     (select-keys b identifying-transaction-props)))

(defn distinct-by [f coll]
  (let [groups (group-by f coll)]
    (map #(first (groups %)) (distinct (map f coll)))))

(defn max-cmp
  "Find the max of a collection using compare"
  [coll]
  (reduce #(if (> (compare %1 %2) 0) %1 %2) coll))

(defn min-cmp
  "Find the min of a collection using compare"
  [coll]
  (reduce #(if (< (compare %1 %2) 0) %1 %2) coll))

(defn extent
  "Finds the max and min of a list of data"
  [xs]
  {:min (min-cmp xs) 
   :max (max-cmp xs)})

(defn get-transactions [query]
  (map #(evolve deserialize-transforms %) (sql/query db query)))

(defn remove-existing-transactions
  "Takes a list of transactions and a db and removes any that 
  are already in the db"
  [db ts]
  (let [{:keys [min max]} (extent (map :transdate ts))
        query ["select * from transactions where transdate >= ? and transdate <= ?" min max]
        potential-duplicates (into #{} (map #(select-keys % identifying-transaction-props)
                                            (get-transactions query)))]
    (remove #(potential-duplicates (select-keys % identifying-transaction-props)) ts)))

(defn csv-data->maps 
  "takes a csv represented as a vector of vectors 
  and returns a seq of maps where the keys are 
  determined by the first row which is assumed to be the header"
  [csv-data]
  (map zipmap
    (repeat (first csv-data))
    (rest csv-data)))

(defn import-csv 
  "takes a filename for a csv and a map that translates 
  the csv column names to database column names and returns
  a map that is ready to be inserted into the database"
  [filename col-map]
  (with-open [reader (io/reader filename)]
    (doall
      (->> (csv/read-csv reader)
           (csv-data->maps)
           (map #(select-keys % (keys col-map)))
           (map #(rename-keys % col-map))
           (map #(evolve parse-transforms %))))))

(def ts (import-csv "data/Activity.CSV" chase-col-map))

(defn add-transactions [ts]
  (let [new-ts (remove-existing-transactions db ts)]
    (if (not (nil? (first new-ts))) 
      (apply sql/insert! db :transactions new-ts))))

(defn ingest-csv
  "imports a csv and saves it to the database"
  [filename col-map]
  (add-transactions (import-csv filename col-map)))

(defn detect-same-test [filename col-map]
  (ingest-csv filename col-map)
  (let [from-file (first (import-csv filename chase-col-map))
        from-db (evolve deserialize-transforms 
                        (first (sql/query db "select * from transactions")))]
    (println from-file)
    (println from-db)
    (same-transaction? from-file from-db)))

(defn avoid-duplicates-test [filename]
  (let [ts (import-csv filename chase-col-map)
        t-count (count ts)]
    (add-transactions (take (/ t-count 2) ts))
    (add-transactions ts)
    (= t-count (:count (first (sql/query db "select count(id) as count from transactions"))))))

(defn -main
  "I don't do a whole lot ... yet."
  [[csv-file col-map]]
  (import-csv csv-file chase-col-map))

