(ns blocks.lambda
  (:require [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.pprint :as pprint]
            [blocks.macro :refer :all]
            [blocks.event :as blks-evt]
            [blocks.util :as blks-util]
            [blocks.error :refer :all]
            [taoensso.timbre :as timbre :refer [log debug info warn error fatal debugf infof warnf]])
  (import [com.amazonaws ClientConfiguration]
          [com.amazonaws.services.lambda AWSLambdaAsyncClient AWSLambdaAsyncClientBuilder AWSLambdaAsync]
          [com.amazonaws.services.lambda.model InvokeRequest InvokeResult]
          [java.nio ByteBuffer]
          [java.util Base64]
          [java.util.concurrent Future ExecutorService Executors ExecutionException])
  (:gen-class))


;;; -------------
;;; - Protocols
;;; -------------


(defprotocol EventDispatcher
  (invoke [_ req] "dispatch event"))

(deftype LambdaEventDispatcher [^AWSLambdaAsync lambda]
  EventDispatcher
  (invoke [_ request] (.invokeAsync lambda request)))

;; Test Dispatcher
(deftype FooDispatcher [executor]
  EventDispatcher
  (invoke [_ req]
     (.submit executor (fn []
       (let [timeout (* (rand-int 10) 1000)]
         (Thread/sleep timeout)
         {:duration timeout})))))

;; ----

(defrecord InvokeRequestPacket [payload ^InvokeRequest request])

(defrecord FuturePacket [payload ^Future future])

(defn ^ClientConfiguration configuration [& {:keys [socket-timeout connection-timeout]
                                             :or {socket-timeout 120
                                                  connection-timeout 120}}]
  (-> (ClientConfiguration. )
      (.withSocketTimeout (* socket-timeout 1000))
      (.withConnectionTimeout (* connection-timeout 1000))))

(defn ^AWSLambdaAsync client
  ([config]
    (.build
      (-> (AWSLambdaAsyncClientBuilder/standard)
          (.withClientConfiguration config))))
  ([]
    (client (configuration))))

(defn ^InvokeRequestPacket invoke-request [func-name payload]
  (let [req (InvokeRequest.)]
    (InvokeRequestPacket.
      payload
      (-> req
        #_(.withInvocationType "Event")
        (.withFunctionName func-name)
	(.withPayload (ByteBuffer/wrap (.getBytes (json/write-str payload))))))))

(defn parse-base64 [^String in]
  (if in
    (String. (.decode (Base64/getDecoder) in))
    in))

(defn parse-payload [^ByteBuffer in]
  (let [raw (String. (.array in))]
    (json/read-str raw)))

(defn check-status [^FuturePacket fut-pkt]
  (try
    (let [^InvokeResult res (.get (.future fut-pkt))
          status-code (.getStatusCode res)
          evt (.payload fut-pkt)]
      (if (= status-code 200)
        (let [payload (parse-payload (.getPayload res))]
          (if-let [err (payload "errorMessage")]
            (do
              (info "--- Lambda Error ---")
              (error (.getFunctionError res))
              (error err)
              (infof "EVENT=%s" (json/write-str evt))
              {:error true :event evt})
            (do
              (infof "Lambda Success: %s" payload)
              (infof "KEY=%s" (get-in evt ["Records" 0 "s3" "object" "key"]))
	      {:error false})))
        (do
          (infof "Received a non-OK response from AWS: %d" status-code)
	  {:error true :event evt})))
    (catch InterruptedException e
      (error e))
    (catch ExecutionException e
      (error e))
    (catch Exception e
      (error e))))

(defn split-queue [status queue]
  "Split queue into completed and pending futures"
  (loop [sts (seq status)
         comp []   ; completed futures
	 pend []]  ; pending futures
    (if-let [[idx done] (first sts)]
      (if done
        (recur (rest sts) (conj comp (nth queue idx)) pend)
	(recur (rest sts) comp (conj pend (nth queue idx))))
      [comp pend])))

(defn check-queue [queue & {:keys [timeout]
                            :or {timeout 2000}}]
  (let [n (count queue)]
    (if (= n 0)
      [[] []]
      (loop [i 0
             at-least-one false
             status {}]
	(if (< i n)
	  (let [done? (.isDone (.future (nth queue i)))]
	    (recur (inc i) (or at-least-one done?) (assoc status i done?)))
          (if at-least-one
            (split-queue status queue)
            (do
              (info ".")
              (try
                (Thread/sleep timeout)
                (catch InterruptedException e
                  (error "Thread/sleep was interrupted!")
                  (error e)))
              (recur 0 false status))))))))

(defn start-request-dispatcher [in-ch ^blocks.lambda.EventDispatcher dispatcher & {:keys [max-queue-size
                                                                                       timeout]
                                                                                :or {max-queue-size 5
                                                                                     timeout 1000}}]
  (let [out-ch (async/chan 1000)
        pending? #(> (count %) 0)
        send-res (fn [xs]
                   (doseq [res xs]
                     (async/>!! out-ch res)))]
    (async/thread
      (loop [queue []]
        (let [i (count queue)]
          (if (< i max-queue-size)
            (if-let [^InvokeRequestPacket packet (async/<!! in-ch)]
              (recur (conj queue (FuturePacket. (.payload packet) (.invoke dispatcher (.request packet)))))
              (let [[comp pend] (check-queue queue)]
                 (send-res comp)
                 (if (pending? pend)
                   (recur pend)
                   (async/close! out-ch))))
            (do
              (debugf "Queue at capacity(%d); waiting..." max-queue-size)
              (let [[comp pend] (check-queue queue)]
                (send-res comp)
                (recur pend))))))
      (info "Request-dispatcher thread done."))
    out-ch))

(defn run [src-ch dispatcher max-queue-size & {:keys [result-handler]
                                               :or {result-handler #(.get %)}}]
  (try
    (let [out-ch (start-request-dispatcher src-ch dispatcher :max-queue-size max-queue-size)]
      (loop [i 1
             errs []]
        (if-let [fut-pkt (async/<!! out-ch)]
         (do
            (infof "=== %d ===" i)
            (let [res (result-handler fut-pkt)]
              (info "___")
              (if (:error res)
                (recur (inc i) (conj errs (:event res)))
                (recur (inc i) errs))))
          (if (> (count errs) 0)
            (let [failures (json/write-str errs)
                  filename (format "%s.json" (blks-util/md5 failures))]
              (infof "Writing failed events to %s" filename)
              (spit filename failures))
            (info "Processing complete without errors")))))
    (catch Exception e
      (print e)
      (System/exit 1)))
    (System/exit 0))

(defn events->request [func-name evts]
  "Returns channel of InvokeRequestPacket records"
  (infof "Loaded %d events" (count evts))
  (async/to-chan (map #(invoke-request func-name %) evts)))

(defn launch-from-events [clnt path func-name max-queue-size]
  (debug "Launching from events source")
  (let [evts (blks-util/load-json path)]
    (run
      (events->request func-name evts)
      (LambdaEventDispatcher. clnt)
      max-queue-size
      :result-handler check-status)))

(defn launch-from-blocks [clnt path func-name max-queue-size]
  (debug "Launching from blocks source")
  (let [evts (map blks-evt/event (blks-util/load-json path))]
    (debug evts)
    (run
      (events->request func-name evts)
      (LambdaEventDispatcher. clnt)
      max-queue-size
      :result-handler check-status)))

(defn launch [clnt src-type path func-name max-queue-size]
  (debugf "SOURCE=%s" src-type)
  (case src-type
    "block" (launch-from-blocks clnt path func-name max-queue-size)
    "event" (launch-from-events clnt path func-name max-queue-size)
    (throw (blocks.error.UnknownDataSourceException. (format "Unknown type: %s" src-type)))))

;;;; ----- MAIN -----

(defn parse-args [args]
  (hash-map
    :src  (nth args 0)
    :path (nth args 1)
    :func-name (nth args 2)
    :max-queue (read-string (nth args 3))))

(defn -main
  [& args]
  (try
    (let [vals (parse-args args)]
      (infof "LAMBDA[%s]" (:func-name vals))
      (infof "SOURCE_PATH=%s" (:path vals))
      (infof "MAX_QUEUE_SIZE=%d" (:max-queue vals))
      (launch (client)
              (:src vals)
              (:path vals)
              (:func-name vals)
              (:max-queue vals)))
    (catch Exception e
      (error e)
      (System/exit 1))
    (finally
      (shutdown-agents)
      (System/exit 0))))
