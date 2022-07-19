(defproject de.active-group/active-events "0.1.1-SNAPSHOT"
  :description "A library for event sourcing."
  :url "http://github.com/active-group/active-events"
  
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[org.clojure/clojure "1.10.1"]
                 [de.active-group/active-clojure "0.41.0"]
                 [de.active-group/active-jdbc "0.2.0"]
                 [com.github.seancorfield/next.jdbc "1.2.780"]]

  :profiles {:dev {:dependencies [[com.h2database/h2 "2.0.202"]]}})
