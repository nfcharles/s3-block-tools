(ns blocks.core
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.pprint :as pprint]
            [blocks.block :as block]
            [blocks.lambda :as lambda])
  (:gen-class))

;; blocks -h copy -h --threads 10 BUCKET PREFIXES FILES EXPRESSION
;; blocks -h lambda -h --queue-size 10 --block --event PATH LAMBDA

(defn parse-list [s]
  (clojure.string/split s #","))


;;; ----------------
;;; -   OPTIONS    -
;;; ----------------

(def global-options
  [["-h" "--help"]])

(def copy-options
  [["-t" "--threads THREAD" "Thread count"
    :default 4
    :parse-fn read-string]
   ["-h" "--help"]])

(def lambda-options
  [["-q" "--queue-size SIZE" "Max event queue size"
    :default 10
    :parse-fn read-string]
   ["-i" "--input-type INPUT_TYPE (\"block\"|\"event\")" "Input file type"
    :default "block"]
   ["-h" "--help"]])


;;; ----------------
;;; -    USAGE     -
;;; ----------------

(defn global-usage [summary]
  (->> ["Utility for retrieving blocks or submitting events for lambda execution."
        ""
        "Usage: blocks [options] copy|lambda"
        ""
        "Options:"
        summary]
        (clojure.string/join "\n")))

(defn copy-usage [summary]
  (->> ["Copies blocks from BUCKET subject to PREFIXES to FILES.  Blocks are"
        "identified by EXPRESSION (regular expression)."
        ""
        "Usage: blocks [options] copy BUCKET PREFIXES FILES EXPRESSION"
        ""
        "Options:"
        summary]
        (clojure.string/join "\n")))

(defn lambda-usage [summary]
  (->> ["Submits events for lambda execution.  At most SIZE events are submitted concurrently."
        ""
        "Usage: blocks [options] lambda PATH FUNCTION"
        ""
        "Options:"
        summary]
        (clojure.string/join "\n")))


;;; -----------------
;;; -    COPY       -
;;; -----------------

(def copy-pos-len 4)

(defn invoke-copy [bkt thrds expr pfxs paths]
  (let [clnt (block/client)]
    (block/launch clnt bkt thrds expr pfxs paths)))

(defn assemble-copy-callable [args opts summary]
  ;(println (format "OPTS=%s" opts))
  (if (= copy-pos-len (count args))
    (let [[bkt pfxs paths expr] args]
      {:fn #(invoke-copy bkt (:threads opts) expr (parse-list pfxs) (parse-list paths))})
    {:error "Not enough positional arguments." :usage (copy-usage summary)}))

(defn parse-copy [args]
  ;(println (format "COPY_ARGS=%s" args))
  (let [{:keys [options arguments errors summary]}
        (parse-opts args copy-options)]
    (cond
      errors          {:error (clojure.string/join "\n" errors)
                       :usage (copy-usage summary)}
      (:help options) {:error nil
                       :usage (copy-usage summary)}
      :else           (assemble-copy-callable arguments options summary))))


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
        {:fn #(invoke-lambda src path func-name (:queue-size opts))})
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
          "copy"   (parse-copy (rest arguments))
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
