(ns http)
(require 'clojure.string)
(import '(java.net InetSocketAddress))
(import '(java.net URLEncoder))
(import '(java.nio ByteBuffer))
(import '(java.nio CharBuffer))
(import '(java.nio.channels Selector))
(import '(java.nio.channels SelectionKey))
(import '(java.nio.channels SocketChannel))
(import '(java.nio.charset Charset))
(import '(java.util.concurrent Callable))
(import '(org.apache.commons.codec.binary Base64))

(defrecord http-request [uri method headers body])
(defrecord http-response [request status status-msg headers body])

(def http-client-version "0.0.0")

(def url-encode
  (fn [s]
    (URLEncoder/encode s "UTF-8")))
  
(def url-encode-params
  (fn [params]
    (clojure.string/join "&"
                         (map #(clojure.string/join "="
                                                     (map
                                                       url-encode
                                                       %))
                              params))))

(defn asbuffers [value]
  (let [value-type (class value)]
    (cond
      (isa? value-type ByteBuffer) [value]
      (isa? value-type String) [(.encode (Charset/forName "UTF-8") (CharBuffer/wrap value))]
      (or (list? value) (vector? value) (seq? value)) (concat (map #(asbuffers %) value))
      :else [])))

(defn dispatch-io [client key]
  (cond (.isConnectable key) nil
        (.isWritable key) nil
        (.isReadable key) nil))

(defprotocol HttpConn
  (read [this])
  (write [this])
  (connect [this]))

(deftype http-conn [client]
  HttpConn
  (read [this] ())
  (write [this] ())
  (connect [this] ()))

(deftype https-conn [client context]
  HttpConn
  (read [this] ())
  (write [this] ())
  (connect [this] ()))

(defn io-loop [client]
  (fn []
    (let [sel (.select (:selector client) 1000)]
      (let [keys (.selectedKeys (:selector client))]
        (map #(dispatch-io client %) keys))
    (.submit (:executor client) (io-loop client)))))

(defn dorequest [client request] 
  (let [body-buffers (asbuffers (:body request))]
    (let [headers (merge (:headers request)
                         (if (> (count body-buffers) 0)
                           {"Content-length" (str (reduce + (map #(.remaining %) body-buffers)))}
                           {})
                         {"Host" (.getHost (:uri request))}
                         {"User-Agent" (str "(http-clojure/" http-client-version " clojure/" (clojure-version) ")")})]
      (let [bufs (concat (asbuffers (str (:method request)
                                         " "
                                         (.getPath (:uri request))
                                         (let [query (.getQuery (:uri request))]
                                           (if (nil? query) ""
                                             (str "?" (.getQuery (:uri request)))))
                                         " HTTP/1.1\r\n"))
                         (asbuffers (str (clojure.string/join
                                           "\r\n"
                                           (map #(str (first  %) ": " (second %)) headers))
                                         "\r\n\r\n"))
                         body-buffers)]
        (case (.getProtocol (:uri request))
          "http" ()
          "https" ())
        (print (map #(.toString (.decode (Charset/forName "UTF-8") (.duplicate %))) bufs))
        bufs))))

(defn base64enc [s]
  (Base64/encodeBase64String (.getBytes s)))

(defn auth-header [auth]
  (if (nil? auth) {}
    {"Authorization" (base64enc (str (first auth) ":" (second auth)))}))

(defprotocol HttpClient
  (get    [this uri headers auth]        "Perform a GET on uri, optinally with headers.")
  (put    [this uri headers body auth]   "Perform a PUT on uri, optionally with headers and body.")
  (post   [this uri headers params auth] "Perform a POST on uri, optionally with headers and a map of parameters, or a string/buffer.")
  (head   [this uri headers auth]        "Perform a HEAD on uri, optionally with headers.")
  (delete [this uri headers auth]        "Perform a DELETE on uri, optionally with headers.")
  (start  [this]                         "Start the IO loop."))

(defrecord client [executor selector]
  HttpClient
  (get [this uri headers auth]
       (dorequest this
                  (http-request. uri "GET"
                                 (merge headers (auth-header auth))
                                 nil)))

  (put [this uri headers body auth]
       (dorequest this
                  (http-request. uri "PUT"
                                 (merge headers (auth-header auth))
                                 body)))

  (post [this uri params headers auth]
        (dorequest this
                   (http-request. uri "POST"
                                  (merge headers (auth-header auth))
                                  (if (map? params)
                                    (url-encode-params params)
                                    params))))
  (head [this uri headers auth]
        (dorequest this
                   (http-request. uri "HEAD" 
                                  (merge headers (auth-header auth))
                                  nil)))
  (delete [this uri headers auth]
          (dorequest this
                     (http-request. uri "DELETE"
                                    (merge headers (auth-header auth))
                                    nil)))
  (start [this]
         (.submit executor (io-loop this))))