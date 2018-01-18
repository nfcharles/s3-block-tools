(ns blocks.core
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.pprint :as pprint]
            [blocks.block :as block]
            [blocks.lambda :as lambda])
  (:gen-class))


(defn parse-list [s]
  (clojure.string/split s #","))

;;; ----------------
;;; -   OPTIONS    -
;;; ----------------

(def global-options
  [["-h" "--help"]])

(def find-options
  [["-t" "--threads THREAD" "Thread count"
    :default 4
    :parse-fn read-string]
   ["-h" "--help"]])

(def lambda-options
  [["-c" "--concurrency CONCURRENCY" "Max concurrent lambda invocations"
    :default 10
    :parse-fn read-string]
   ["-i" "--input-type INPUT_TYPE (\"block\"|\"event\")" "Input file type"
    :default "block"]
   ["-h" "--help"]])


;;; ----------------
;;; -    USAGE     -
;;; ----------------

(defn global-usage [summary]
  (->> ["Utility for identifying block files and invoking lambda functions with s3 events."
        ""
        "Usage: blocks [options] find|lambda"
        ""
        "Options:"
        summary]
        (clojure.string/join "\n")))

(defn find-usage [summary]
  (->> ["Identifies block files from BUCKET filtered by PREFIXES.  Block files from each PREFIX"
        "are copied to FILES respectively.  Block files are identified by EXPRESSION (regexp)."
        ""
        "Usage: blocks [options] find BUCKET PREFIXES FILES EXPRESSION"
        ""
        "Options:"
        summary]
        (clojure.string/join "\n")))

(defn lambda-usage [summary]
  (->> ["Submits events for lambda execution.  At most CONCURRENCY events are executed concurrently."
        ""
        "Usage: blocks [options] lambda PATH FUNCTION"
        ""
        "Options:"
        summary]
        (clojure.string/join "\n")))


;;; -----------------
;;; -    FIND       -
;;; -----------------

(def find-pos-len 4)

(defn invoke-find [bkt thrds expr pfxs paths]
  (let [clnt (block/client)]
    (block/launch clnt bkt thrds expr pfxs paths)))

(defn assemble-find-callable [args opts summary]
  ;(println (format "OPTS=%s" opts))
  (if (= find-pos-len (count args))
    (let [[bkt pfxs paths expr] args]
      {:fn #(invoke-find bkt (:threads opts) expr (parse-list pfxs) (parse-list paths))})
    {:error "Not enough positional arguments." :usage (find-usage summary)}))

(defn parse-find [args]
  ;(println (format "FIND_ARGS=%s" args))
  (let [{:keys [options arguments errors summary]}
        (parse-opts args find-options)]
    (cond
      errors          {:error (clojure.string/join "\n" errors)
                       :usage (find-usage summary)}
      (:help options) {:error nil
                       :usage (find-usage summary)}
      :else           (assemble-find-callable arguments options summary))))


;;; -----------------
;;; -    LAMBDA     -
;;; -----------------

(def lambda-pos-len 2)

(defn invoke-lambda [src path func-name q-size]
  (let [clnt (lambda/client)]
    (lambda/launch clnt src path func-name q-size)))

(defn parse-source [src]
  (if (contains? #{"block" "event"} src)
    src))

(defn assemble-lambda-callable [args opts summary]
  ;(println (format "OPTS=%s" opts))
  (if (= lambda-pos-len (count args))
    (if-let [src (parse-source (:input-type opts))]
      (let [[path func-name] args]
        {:fn #(invoke-lambda src path func-name (:concurrency opts))})
      {:error (format "Invalid --input-type: \"%s\"" (:input-type opts)) :usage (lambda-usage summary)})
    {:error "Not enough positional arguments." :usage (lambda-usage summary)}))

(defn parse-lambda [args]
  ;(println (format "LAMBDA_ARGS=%s" args))
  (let [{:keys [options arguments errors summary]}
        (parse-opts args lambda-options)]
    (cond
      errors          {:error (clojure.string/join "\n" errors)
                       :usage (lambda-usage summary)}
      (:help options) {:usage (lambda-usage summary)}
      :else           (assemble-lambda-callable arguments options summary))))


;;; -----------------
;;; -    GENERAL    -
;;; -----------------

(defn parse [args opts]
  (let [{:keys [options arguments errors summary]}
        (parse-opts args opts :in-order true)]
    ;(println (format "OPTS=%s\nARGS=%s\nERR=%s\nSUM=%s\n" options arguments errors summary))
    (if (:help options)
      {:usage (global-usage summary)}
      (if-let [cmd (first arguments)]
        (case cmd
          "find"   (parse-find (rest arguments))
          "lambda" (parse-lambda (rest arguments))
          {:error (format "Unknown command: %s" cmd)
           :usage (global-usage summary)})
        {:usage (global-usage summary)}))))


(defn handle-error [in]
  (println (format "%s\n-----\n" (:error in)))
  (println (:usage in))
  (System/exit 1))

(defn handle-usage [in]
  (println (:usage in))
  (System/exit 0))


(defn -main
  [& args]
  (let [ret (parse args global-options)]
    (try
      (cond
        (:error ret) (handle-error ret)
        (:usage ret) (handle-usage ret)
        :else        ((:fn ret)))
      (catch Exception e
        (println e)
        (System/exit 1)))))
