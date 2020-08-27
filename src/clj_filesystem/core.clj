(ns clj-filesystem.core
  (:require [minio-clj.core :as mc]
            [oss-clj.core :as oss]
            [clojure.tools.logging :as log]
            [clojure.string :as str]))

(def services
  ;; TODO: services can't be a private when it is used in with-conn macro.
  (atom {:oss nil
         :minio nil
         :s3 nil}))

(def service
  (atom nil))

(def ^:dynamic *conn*)

(defn- get-current-conn
  []
  (if (bound? #'*conn*)
    *conn*
    (throw (Exception. "Not in with-conn environment"))))

(def ^:private service-map
  {:minio {:make-bucket      mc/make-bucket
           :connect          mc/connect
           :list-buckets     mc/list-buckets
           :put-object       mc/put-object
           :get-object       mc/get-object
           :download-object  mc/download-object
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
           :download-object  oss/download-object
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

(defn- get-fn
  [fn-keyword]
  (fn-keyword (service-map (keyword @service))))

(defn get-protocol
  []
  (protocol-map (keyword @service)))

(defn connect
  []
  (let [conn-info (@services (keyword @service))
        {:keys [fs-endpoint fs-access-key fs-secret-key]} conn-info]
    (log/debug (format "Connect service: %s %s" @service conn-info))
    (if conn-info
      ((get-fn :connect) fs-endpoint fs-access-key fs-secret-key)
      (throw (Exception. "Need to run setup-connection function firstly.")))))

(defn setup-connection
  [fs-service fs-endpoint fs-access-key fs-secret-key]
  (let [conn {:fs-endpoint fs-endpoint
              :fs-access-key fs-access-key
              :fs-secret-key fs-secret-key}]
    (reset! service fs-service)
    (reset! services (merge @services
                            {(keyword fs-service) conn}))))

(defmacro swallow-exceptions [& body]
  `(try ~@body (catch Exception e#)))

(defmacro with-conn
  [fs-service & body]
  `(let [fs-service# ~fs-service]
     (reset! service fs-service#)
     (binding [*conn* (connect)]
       (try
         (when *conn* ~@body)
         (catch Exception e# (str "Exception: " (.getMessage e#)))
         ;; TODO: How to avoid leak memory?
         ;; The MinioClient has not shutdown method.
         (finally (swallow-exceptions (.shutdown *conn*)))))))

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

(defn download-object
  [bucket key localpath]
  ((get-fn :download-object) (get-current-conn) bucket key localpath))

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
                 :path (str (get-protocol) bucket "/" (:key object)))) objects))

(defn correct-file-path
  "When you use minio service, all file paths need to reset as the local path.
   e.g. minio://bucket-name/object-key --> /datains/minio/bucket-name/object-key

   ; TODO: need to support more types for e's value.
  "
  [e fs-rootdir]
  (let [protocol "minio://"
        prefix   (str/replace fs-rootdir #"([^\/])$" "$1/")
        pattern  (re-pattern protocol)
        func     (fn [string] (str/replace string pattern prefix))]
    (into {}
          (map (fn [[key value]]
                 (vector key
                         (cond
                           (map? value) (correct-file-path value fs-rootdir)
                           (vector? value) (map #(func %) value)
                           (string? value) (func value)
                           :else value))) e))
    e))

(defn correct-file-path-reverse
  "When you use minio service, all file paths need to reset as the local path.
   e.g. /datains/minio/bucket-name/object-key --> minio://bucket-name/object-key

   ; TODO: need to support more types for e's value.
  "
  [e fs-rootdir]
  (let [protocol "minio://"
        pattern  (re-pattern (str/replace fs-rootdir #"([^\/])$" "$1/"))
        func     (fn [string] (str/replace string pattern protocol))]
    (into {}
          (map (fn [[key value]]
                 (vector key
                         (cond
                           (map? value) (correct-file-path value fs-rootdir)
                           (vector? value) (map #(func %) value)
                           (string? value) (func value)
                           :else value))) e))
    e))
