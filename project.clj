(defproject de.active-group/active-events "1.0.0"
  :description "A library for event sourcing."
  :url "http://github.com/active-group/active-events"
  
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[org.clojure/clojure "1.10.1"]
                 [de.active-group/active-clojure "0.41.0"]
                 [de.active-group/active-jdbc "0.2.1"]
                 [com.github.seancorfield/next.jdbc "1.2.780"]
                 [org.clojure/data.json "2.4.0"]]

  :profiles {:dev {:dependencies [[com.h2database/h2 "2.0.202"]]}})
