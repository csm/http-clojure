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
(import '(java.util.concurrent Executors))
(import '(javax.net.ssl SSLContext))
(import '(javax.net.ssl SSLEngine))
(import '(javax.net.ssl SSLEngineResult$HandshakeStatus))
(import '(org.apache.commons.codec.binary Base64))

(defrecord http-request [uri method headers body])
(defrecord http-response [request status status-msg headers body])
(defrecord http-error [request result])

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

(defprotocol BufferList
  (get-buffers [_])
  (add-buffer [_ buffer])
  (size [_])
  (to-seq [_]))

(defn align [n m]
  (case (mod n m)
    0 n
    (+ n (- m (mod n m)))))

(defn grow [buffer need]
  (if (nil? buffer)
    (ByteBuffer/allocate (align need 1024))
    (if (> need (.remaining buffer))
      (.put (ByteBuffer/allocate (align need 1024)) (.flip (.duplicate buffer)))
      buffer)))

(defn dupe [buffer]
  (.flip (.put (ByteBuffer/allocate (.remaining buffer)) buffer)))

(defn asbuffers [value]
  (let [value-type (class value)]
    (cond
      (isa? value-type ByteBuffer) [value]
      (isa? value-type String) [(.encode (Charset/forName "UTF-8") (CharBuffer/wrap value))]
      (or (list? value) (vector? value) (seq? value)) (concat (map #(asbuffers %) value))
      :else [])))

(defn dispatch-io [client key]
  (cond (.isConnectable key) (connect (.getAttachment key) key)
        (.isWritable key) (write (.getAttachment key) key)
        (.isReadable key) (read (.getAttachment key) key)))

(defprotocol HttpConn
  (read [this key])
  (write [this key])
  (connect [this key]))

(deftype http-conn [client request channel response-promise ^{:volatile-mutable true} buffer]
  HttpConn
  (read [this k]
        (do
          (set! buffer (grow buffer 1024))
          (let [n (.read channel buffer)]
            (cond
              (< n 0) (deliver response-promise (http-error. request "read failed"))
              (= n 0) nil
              (> n 0) nil))))
  (write [this k]
         (let [buf (first (filter #(> (.remaining %) 0) (:body request)))]
           (case buf
             nil (.interestOps k SelectionKey/OP_READ)
             (.write channel buf))))
  (connect [this k]
           (if (.finishConnect channel)
             (.interestOps k SelectionKey/OP_WRITE)
             (deliver response-promise (http-error. request "could not connect to host")))))

(deftype https-conn [client request channel engine response-promise ^{:volatile-mutable true} buffer]
  HttpConn
  (read [this k]
        (case (.getHandshakeStatus engine)
          SSLEngineResult$HandshakeStatus/NEED_UNWRAP (do
                                                        (set! buffer (grow buffer 1024))
                                                        (.read channel buffer)
                                                        (.unwrap engine (.flip buffer))
                                                        (.compact buffer))
          SSLEngineResult$HandshakeStatus/NEED_WRAP (.interestOps k SelectionKey/OP_WRITE)
          SSLEngineResult$HandshakeStatus/NEED_TASK (.run (.getDelegatedTask engine))
          SSLEngineResult$HandshakeStatus/NOT_HANDSHAKING (do
                                                            (set! buffer (grow buffer 1024))
                                                            (.read channel buffer)
                                                            (.unwrap engine buffer))))
  (write [this k] ())
  (connect [this k]
           (if (.finishConnect channel)
             (do
               (.beginHandshake engine)
               (case (.getHandshakeStatus engine)
                 SSLEngineResult$HandshakeStatus/NEED_WRAP (.interestOps k SelectionKey/OP_WRITE)
                 SSLEngineResult$HandshakeStatus/NEED_UNWRAP (.interestOps k SelectionKey/OP_READ)
                 SSLEngineResult$HandshakeStatus/NEED_TASK (.run (.getDelegatedTask engine))
                 SSLEngineResult$HandshakeStatus/NOT_HANDSHAKING (.interestOps k SelectionKey/OP_WRITE)))
             (deliver response-promise (http-error. request "could not connect to host")))))

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
                                           (map #(str (first  %) ": " (second %)) headers)
                                         "\r\n\r\n"))
                         body-buffers))
            request-promise (promise)]
        (case (.getProtocol (:uri request))
          "http" ((let [host (.getHost (:uri request))
                        port (if (> (.getPort (:uri request)) 0)
                               (.getPort (:uri request))
                               80)]
                    (let [conn (http-conn. client request (SocketChannel/open) request-promise (buffer-list. ()))]
                    (do
                      (.configureBlocking (.socket (:channel conn)) false)
                      (.connect 
                        (:channel conn)
                        (InetSocketAddress. host port))
                      (.register (:channel conn)
                        (:selector client)
                        SelectionKey/OP_CONNECT
                        conn)))))
          "https" ((let [host (.getHost (:uri request))
                         port (if (> (.getPort (:uri request)) 0)
                                (.getPort (:uri request))
                                443)]
                     (let [conn (https-conn. client
                                             request
                                             (SocketChannel/open)
                                             (.createSSLEngine
                                               (or (:sslcontext client) (SSLContext/getDefault))
                                               host port)
                                             request-promise
                                             (buffer-list. ()))]
                       (do
                         (.configureBlocking (.socket (:channel conn)) false)
                         (.connect (:channel conn)
                           (InetSocketAddress. host port))
                         (.register (:channel conn)
                           (:selector client)
                           SelectionKey/OP_CONNECT
                           conn))))))
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

(defrecord client [executor selector sslcontext]
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

(defn make-http-client [& nthreads]
  (client. (Executors/newCachedThreadPool (or nthreads (* (.availableProcessors (Runtime/getRuntime)) 2)))
           (Selector/open)
           (SSLContext/getDefault)))
               