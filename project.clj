(defproject clj-filesystem "0.1.0"
  :description "An unified sdk for several object storage system."
  :url "https://github.com/clinico-omics/clj-filesystem"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [minio-clj "0.1.0"]
                 [oss-clj "0.1.0"]]
  :repl-options {:init-ns clj-filesystem.core})
