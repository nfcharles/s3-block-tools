(ns blocks.app
  (:require [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.pprint :as pprint]
	    [blocks.macro :refer :all]
	    [taoensso.timbre :as timbre :refer [log debug info warn error fatal debugf infof warnf]])
  (import [com.amazonaws.services.lambda AWSLambdaAsyncClientBuilder AWSLambdaAsync]
          [com.amazonaws.services.lambda.model InvokeRequest InvokeResult]
	  [java.nio ByteBuffer]
	  [java.util.concurrent Future ExecutorService Executors ExecutionException])
  (:gen-class))


(defn ^AWSLambdaAsync lambda-client []
  (AWSLambdaAsyncClientBuilder/defaultClient))

(defn ^InvokeRequest invoke-request [func-name payload]
  (let [req (InvokeRequest.)]
    (-> req
        (.withFunctionName func-name)
	(.withPayload (ByteBuffer/wrap (.getBytes payload))))))

(defn check-status [fut]
  (try
    (let [res (.get fut)
          status-code (.getStatusCode res)]
      (if (= status-code 200)
        (let [payload (.getPayload res)]
          (info "Lambda function returned:")
          (info (String. (.array payload))))
        (infof "Received a non-OK response from AWS: %d" status-code)))
    (catch InterruptedException e
      (error e)
      (System/exit 1))
    (catch ExecutionException e
      (error e)
      (System/exit 1))))

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
                            :or {timeout 1000}}]  ;; TODO: make timeout default to 1000 ms
  (let [n (count queue)]
    (if (= n 0)
      [[] []]
      (loop [i 0
             at-least-one false
             status {}]
	(if (< i n)
	  (let [state (.isDone (nth queue i))]
	    (recur (inc i) (or at-least-one state) (assoc status i state)))
          (if at-least-one
            (split-queue status queue)
            (do
              (info ".")
              (try
                (Thread/sleep timeout)
                (catch InterruptedException e
                  (error "Thread/sleep was interrupted!")
                  (error e)
                  (System/exit 1)))
              (recur 0 false status))))))))
	      
(defn start-request-dispatcher [in-ch dispatcher & {:keys [max-queue-size
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
            (if-let [req (async/<!! in-ch)]
              (recur (conj queue (.invoke dispatcher req)))
              (do
                (info "Request stream exhausted; polling queue.")
                (let [[comp pend] (check-queue queue)]
                   (send-res comp)
                   (if (pending? pend)
                     (recur pend)
                     (async/close! out-ch)))))
            (do
              (infof "Queue at capacity(%d); waiting..." max-queue-size)
              (let [[comp pend] (check-queue queue)]
                (send-res comp)
                (recur pend))))))
      (info "Request-dispatcher thread done."))
    out-ch))

(defn start-request-loader [func-name blocks & {:keys [buffer-size
                                                       serializer]
                                                :or {buffer-size 500
						     serializer #(json/write-str %)}}]
  (let [out-ch (async/chan buffer-size)]
    (async/thread
      (doseq [block blocks]
        (async/>!! out-ch (invoke-request func-name (serializer block))))
      (async/close! out-ch))
    out-ch))


(defn load-blocks [source]
  (json/read-str (slurp source)))

(defn launch [source dispatcher func-name event-serializer max-queue-size & {:keys [result-handler]
                                                                             :or {result-handler #(.get %)}}]
  (let [blocks (load-blocks source)
        out-ch (-> (start-request-loader func-name blocks :serializer event-serializer)
                   (start-request-dispatcher dispatcher :max-queue-size max-queue-size))]
    (loop [i 1]
      (if-let [res (async/<!! out-ch)]
        (do
          (infof ">>>>>>>>>> (%d) <<<<<<<<<<" i)
          (result-handler res)
          (recur (inc i)))))))



;;; -------------
;;; - Protocols
;;; -------------

;;; Lambda Dispatcher

(defprotocol EventDispatcher
  (invoke [_ req] "dispatch event"))

(deftype LambdaEventDispatcher [lambda]
  EventDispatcher
  (invoke [_ request] (.invokeAsync lambda request)))

(defn result-handler [res]
  (check-status res))

(defn test-lambda [source func-name max-queue-size evt-ser]
  (let [dispatch (LambdaEventDispatcher. (lambda-client))]
    (launch source dispatch func-name evt-ser max-queue-size :result-handler check-status)
    (System/exit 0)))


;;; Test Dispatcher

(comment
(defprotocol TestEventDispatcher
  (invoke [_ req] "send test event"))
)

(deftype FooDispatcher [executor]
  EventDispatcher
  (invoke [_ req]
     (.submit executor (fn []
       (let [timeout (* (rand-int 10) 1000)]
         (Thread/sleep timeout)
         {:duration timeout})))))

(defn test-dispatch [source func-name max-queue-size threads]
  (with-pool [pool (Executors/newFixedThreadPool threads)]
    (let [dispatch (FooDispatcher. pool)
          evt-ser #(json/write-str %)
          handler (fn [res]
                    (let [ret (.get res)]
                      (infof "RESULT=%s" ret)))]
      (launch source dispatch func-name evt-ser max-queue-size :result-handler handler))))


;;;; ----- MAIN -----

;;; UDFs

(defn encode [s]
  (clojure.string/replace s #"=" "%3D"))

(defn s3-event [blk]
  {"Records" [
    {"s3" {
      "object" {
        "key" (encode (blk "id"))
       }
       "bucket" {
         "name" (blk "bkt")
       }}}]})

(defn block->event [blk]
  (json/write-str (s3-event blk)))

(defn test-evt-ser [source]
  (let [blocks (load-blocks source)]
    (doseq [blk blocks]
      (info (block->event blk)))))

(defn -main
  [& args]
  (let [source    (nth args 0)
        func-name (nth args 1)
        max-queue (read-string (nth args 2))
        lambda    (lambda-client)
        threads   3]
    (infof "LAMBDA[%s]" func-name)
    (infof "SOURCE=%s" source)
    (infof "MAX_QUEUE_SIZE=%d" max-queue)
    (try
      ;(test-dispatch source func-name max-queue threads)
      (test-lambda source func-name max-queue block->event)
      ;(test-evt-ser source)
      (finally
        (shutdown-agents)))))
