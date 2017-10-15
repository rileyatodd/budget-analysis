(defproject budget-analysis "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/java.jdbc "0.3.7"]
                 [org.xerial/sqlite-jdbc "3.7.2"]
                 [org.clojure/data.csv "0.1.4"]
                 [clj-time "0.14.0"]]
  :main ^:skip-aot budget-analysis.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
