(ns blocks.block
  (:require [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.pprint :as pprint]
            [blocks.macro :refer :all]
            [taoensso.timbre :as timbre :refer [log debug info warn error fatal debugf infof warnf]])
  (import [org.apache.hadoop.fs.s3 Jets3tFileSystemStore INode]
	  [org.apache.hadoop.fs Path]	  
	  [org.jets3t.service S3Service]
          [org.jets3t.service.impl.rest.httpclient RestS3Service]
	  [org.jets3t.service.model S3Bucket S3Object]
	  [org.jets3t.service.security AWSCredentials]
	  [com.amazonaws.auth DefaultAWSCredentialsProviderChain]
	  [java.net URI URLEncoder]
	  [java.util.concurrent Executors ExecutorCompletionService])
  (:gen-class))


(defn credentials []
  (let [creds (.getCredentials (DefaultAWSCredentialsProviderChain.))]
    (hash-map
      :access-key (.getAWSAccessKeyId creds)
      :secret-key (.getAWSSecretKey creds))))

(defn service [access-key secret-key]
  (let [aws-creds (AWSCredentials. access-key secret-key)]
    (RestS3Service. aws-creds)))

(defn client
  ([creds]
    (service (:access-key creds) (:secret-key creds)))
  ([]
    (client (credentials))))

(def file-system-name "fs")
(def file-system-value "Hadoop")
(def file-system-type-name "fs-type")
(def file-system-type-value "block")
(def file-system-version-name "fs-version")
(def file-system-version-value "1")

(def metadata
  (hash-map
    "fs" "Hadoop"
    "fs-type" "block"
    "fs-version" "1"))

(defn -field [s3Obj field]
  (.getMetadata s3Obj field))

(defn check-metadata [s3obj]
  (let [name (-field file-system-name)
        type (-field file-system-type-name)
	dver (-field file-system-version-name)]
    (if (not= file-system-value name)
      (throw (java.lang.Exception. "Not a Hadoop S3 file.")))
    (if (not= file-system-type-value type)
      (throw (java.lang.Exception. "Not a block file")))
    (if (not= file-system-version-value dver)
      (throw (java.lang.Exception.
               (format "Version mismatch: %d != %d" file-system-version-value dver))))))
  
(defn bucket [name]
  (S3Bucket. name))

(defn -get-inode [clnt bkt key meta?]
  (let [obj (.getObject clnt bkt key)]
    (if meta?
      (check-metadata obj))
    (.getDataInputStream obj)))
    
(defn inode [clnt bkt path]
  (INode/deserialize (-get-inode clnt bkt path false)))

(defn gen-filter [expr]
  (fn [key]
    (let [res (re-matches (re-pattern expr) key)]
      res)))

(defn block-name [id]
  (str "block_" id))

(defn record [bkt blk]
  (hash-map
    :bkt bkt
    :id  (block-name (.getId blk))
    :len (.getLength blk)))

(defn get-blocks [clnt bkt path]
  (let [bkt-name (.getName bkt)]
    (loop [blks (.getBlocks (inode clnt bkt path))
           acc []]
      (if-let [blk (first blks)]
        (recur (rest blks) (conj acc (record bkt-name blk)))
        acc))))

(defn list-objects [clnt bkt-name pfx & {:keys [delim filter]
                                         :or {delim nil
					      filter (fn [x] true)}}]
  (let [ret (.listObjects clnt bkt-name pfx delim)]
    (infof "count.keys.unfiltered %d" (count ret))
    (loop [objs ret
           acc []]
      (if-let [o (first objs)]
        (let [key (.getKey o)]
          (if (filter key)
	    (recur (rest objs) (conj acc key))
	    (recur (rest objs) acc)))
        acc))))

(defn -get-from-svc [csvc]
  (try
    (.get (.take csvc))
    (catch java.lang.Exception e
      (error e))))

(defn assemble-blocks [csvc n & {:keys [progress-interval]
                                 :or {progress-interval 50}}]
  (loop [i n
         acc []]
    (if (> i 0)
      (if-let [res (-get-from-svc csvc)]
        (let [remain (dec i)]
          (if (= 0 (mod remain progress-interval))
	    (infof "%d blocks remaining" remain))
          (recur remain (conj acc res))))
      (do
        (infof "Retrieved %d blocks" n)
        (flatten acc)))))

(defn start-block-reader [threads clnt bkt in-ch]
  (let [out-ch (async/chan 10)]
    (async/thread
      (infof "Starting block reader...")
      (with-pool [pool (Executors/newFixedThreadPool threads)]
        (time
          (let [csvc (ExecutorCompletionService. pool)]
            (loop []	  
              (if-let [payload (async/<!! in-ch)]
                (let [keys (:keys payload)]
                  (infof "Getting blocks for prefix=%s..." (:pfx payload))
                  (doseq [key keys]
                    (.submit csvc #(get-blocks clnt bkt key)))
                  (async/>!! out-ch (assemble-blocks csvc (count keys)))
		  (recur))
                (async/close! out-ch))))))
      (info "Block Reader done."))
  out-ch))
    
(defn start-prefix-reader [clnt bkt expr pfxs]
  (let [out-ch (async/chan 10)]
    (async/thread
      (info "Starting key reader...")
      (doseq [pfx pfxs]
        (let [keys (list-objects clnt bkt pfx :filter (gen-filter expr))]
          (infof "count.keys.filtered %d" (count keys))
	  (async/>!! out-ch {:pfx pfx :keys keys})))
      (async/close! out-ch)
      (info "Key Reader done."))
    out-ch))

(defn write-blocks [path blks]
  (let [f (format "%s.json" path)]
    (infof "Writing blocks file \"%s\"" f)
    (spit f (json/write-str blks))
    (info "done")))

(defn write [paths in-ch]
  (loop [xs paths]
    (if-let [blks (async/<!! in-ch)]
      (do
        (write-blocks (first xs) blks)
	(recur (rest xs))))))

(defn launch [clnt bkt-name thrds expr pfxs paths]
  (let [bkt (bucket bkt-name)
        out-ch (->> (start-prefix-reader clnt bkt expr pfxs)
                    (start-block-reader thrds clnt bkt))]
    (write paths out-ch)))


;;;; ----- MAIN -----

(defn parse-args [args]
  (hash-map
    :threads  (read-string (nth args 0))
    :bucket   (nth args 1)
    :prefixes (clojure.string/split (nth args 2) #",")
    :paths    (clojure.string/split (nth args 3) #",")
    :expr     (nth args 4)))

(defn -main
  [& args]
  (try
    (let [vals (parse-args args)]
      (infof "BUCKET=%s" (:bucket vals))
      (infof "PREFIX=%s" (:prefixes vals))
      (infof "NAMES=%s"  (:paths vals))
      (infof "BLOCK_EXPRESSION=%s" (:expr vals))
      (launch (client)
              (:bucket vals)
              (:threads vals)
              (:expr vals)
              (:prefixes vals)
              (:paths vals)))
    (catch Exception e
      (error e))))
