(ns blocks.event
  (:require [blocks.util :as blks-util]
            [clojure.data.json :as json]
            [taoensso.timbre :as timbre :refer [log debug info  debugf infof]])
  (:gen-class))


(defn encode [s]
  (clojure.string/replace s #"=" "%3D"))

(defn event [blk]
  {"Records" [
    {"s3" {
       "object" {
         "key" (encode (blk "id"))
       }
       "bucket" {
         "name" (blk "bkt")
       }}}]})


(defn -main
  [& args]
  (let [path (nth args 0)
        filename (nth args 1)
	blks (blks-util/load-json path)]
    (infof "SOURCE=%s" path)
    (infof "Loaded %d blocks" (count blks))
    (loop [xs blks
           acc []]
      (if-let [blk (first xs)]
        (recur (rest xs) (conj acc (event blk)))
	(do
          (infof "Writing events file %s" filename)
          (spit filename (json/write-str acc)))))))
