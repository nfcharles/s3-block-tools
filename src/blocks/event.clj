(ns blocks.event
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
