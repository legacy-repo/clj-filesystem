(ns clj-filesystem.core
  (:require [minio-clj.core :as mc]
            [oss-clj.core :as oss]
            ; [clojure.tools.logging :as log]
            [clojure.string :as str]))

(def ^:dynamic *conn*)

(def connection-pool
  ;; TODO: Connection-pool can't be a private when it is used in with-conn macro.
  (atom {:oss nil
         :minio nil
         :s3 nil}))

(def ^:private default-conn
  (atom nil))

(defn- get-current-conn
  []
  (if (bound? #'*conn*)
    *conn*
    @default-conn))

(def ^:private default-service
  (atom "minio"))

(def ^:dynamic *service*)

(defn- get-current-service
  []
  (keyword (if (bound? #'*service*)
             *service*
             @default-service)))

(def ^:private service-map
  {:minio {:make-bucket      mc/make-bucket
           :connect          mc/connect
           :list-buckets     mc/list-buckets
           :put-object       mc/put-object
           :get-object       mc/get-object
           :list-objects     mc/list-objects
           :remove-bucket    mc/remove-bucket!
           :remove-object    mc/remove-object!
           :get-upload-url   mc/get-upload-url
           :get-download-url mc/get-download-url
           :get-object-meta  mc/get-object-meta}
   :oss   {:make-bucket      oss/make-bucket
           :connect          oss/connect
           :list-buckets     oss/list-buckets
           :put-object       oss/put-object
           :get-object       oss/get-object
           :list-objects     oss/list-objects
           :remove-bucket    oss/remove-bucket!
           :remove-object    oss/remove-object!
           :get-upload-url   oss/get-upload-url
           :get-download-url oss/get-download-url
           :get-object-meta  oss/get-object-meta}})

(def ^:private protocol-map
  {:minio "minio://"
   :s3    "s3://"
   :oss   "oss://"})

(defn ^:private get-fn
  [fn-keyword]
  (fn-keyword (service-map (get-current-service))))

(defn- get-protocol
  []
  (protocol-map (get-current-service)))

(defn setup-connection
  [fs-service fs-endpoint fs-access-key fs-secret-key {:keys [default?]
                                                       :or {default? true}}]
  (binding [*service* fs-service]
    (let [conn ((get-fn :connect) fs-endpoint fs-access-key fs-secret-key)]
      (when default? (reset! default-conn conn))
      (reset! connection-pool (merge @connection-pool
                                     {(keyword fs-service) conn})))))

(defmacro with-conn
  [fs-service & body]
  `(let [fs-service# ~fs-service]
     (binding [*conn* ((keyword fs-service#) @connection-pool)
               *service* fs-service#]
       (when *conn* ~@body))))

(defn make-bucket!
  [^String name]
  ((get-fn :make-bucket) (get-current-conn) name))

(defn list-buckets
  []
  ((get-fn :list-buckets) (get-current-conn)))

(defn put-object!
  ([^String bucket ^String file-name]
   ((get-fn :put-object) (get-current-conn) bucket file-name))
  ([^String bucket ^String upload-name ^String source-file-name]
   ((get-fn :put-object) (get-current-conn) bucket upload-name source-file-name)))

(defn get-object
  [bucket key]
  ((get-fn :get-object) (get-current-conn) bucket key))

(defn list-objects
  ([bucket]
   (list-objects bucket ""))
  ([bucket prefix]
   ((get-fn :list-objects) (get-current-conn) bucket prefix))
  ([bucket prefix recursive]
   ((get-fn :list-objects) (get-current-conn) bucket prefix recursive)))

(defn remove-bucket!
  [bucket]
  ((get-fn :remove-bucket) (get-current-conn) bucket))

(defn remove-object!
  [bucket key]
  ((get-fn :remove-object) (get-current-conn) bucket key))

(defn get-upload-url
  [bucket key]
  ((get-fn :get-upload-url) (get-current-conn) bucket key))

(defn get-download-url
  [bucket key]
  ((get-fn :get-download-url) (get-current-conn) bucket key))

(defn get-object-meta
  [bucket key]
  ((get-fn :get-object-meta) (get-current-conn) bucket key))

(defn format-objects
  [bucket objects]
  (pmap (fn [object]
          (assoc object
                 :Path (str (get-protocol) bucket "/" (:Key object)))) objects))

(defn correct-file-path
  "When you use minio service, all file paths need to reset as the local path.
   e.g. minio://bucket-name/object-key --> /datains/minio/bucket-name/object-key

   ; TODO: need to support more types for e's value.
  "
  [e fs-rootdir]
  (let [protocol (get-protocol)
        prefix   (str/replace fs-rootdir #"([^\/])$" "$1/")
        pattern  (re-pattern protocol)
        func     (fn [string] (str/replace string pattern prefix))]
    (if (= (get-current-service) :minio)
      (into {}
            (map (fn [[key value]]
                   (vector key
                           (cond
                             (map? value) (correct-file-path value fs-rootdir)
                             (vector? value) (map #(func %) value)
                             (string? value) (func value)
                             :else value))) e))
      e)))

(defn correct-file-path-reverse
  "When you use minio service, all file paths need to reset as the local path.
   e.g. /datains/minio/bucket-name/object-key --> minio://bucket-name/object-key

   ; TODO: need to support more types for e's value.
  "
  [e fs-rootdir]
  (let [protocol (get-protocol)
        pattern  (re-pattern (str/replace fs-rootdir #"([^\/])$" "$1/"))
        func     (fn [string] (str/replace string pattern protocol))]
    (if (= (get-current-service) :minio)
      (into {}
            (map (fn [[key value]]
                   (vector key
                           (cond
                             (map? value) (correct-file-path value fs-rootdir)
                             (vector? value) (map #(func %) value)
                             (string? value) (func value)
                             :else value))) e))
      e)))
