(ns csv-utils.lib
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as clj-str])
  (:import [org.apache.commons.io.input BOMInputStream]))

(set! *warn-on-reflection* true)

(defn csv-data->maps [csv-data]
  (map zipmap
       (->> (first csv-data) ;; First row is the header
            (map keyword)    ;; Drop if you want string keys instead
            repeat)
       (rest csv-data)))

(defn bom-reader
  "Remove `Byte Order Mark` and return reader"
  [filepath]
  (-> filepath
      io/input-stream
      BOMInputStream.
      io/reader))

(defn guess-separator
  [filepath]
  (with-open [^java.io.Closeable reader (bom-reader filepath)]
    (let [header (first (line-seq reader))
          seps [\tab \, \; \space]
          sep-map (->> (map #(hash-map % (count (clj-str/split header (re-pattern (str %))))) seps)
                       (into {}))]
      (key (apply max-key val sep-map)))))

(defn read-csv
  [^String file]
  (when (.isFile (io/file file))
    (with-open
     [reader (io/reader file)]
      (doall
       (->> (csv/read-csv reader :separator (guess-separator file))
            csv-data->maps)))))

(defn vec-remove
  "Remove elem in coll"
  [pos coll]
  (vec (concat (subvec coll 0 pos) (subvec coll (inc pos)))))

(defn write-csv!
  "Write row-data to a csv file, row-data is a vector that each element is a map."
  [path row-data]
  (let [columns (keys (first row-data))
        headers (map name columns)
        rows (mapv #(mapv % columns) row-data)]
    (with-open [file (io/writer path)]
      (csv/write-csv file (cons headers rows) :separator \tab))))

(defn write-csv-by-cols! [path row-data columns]
  (let [headers (map name columns)
        rows (mapv #(mapv % columns) row-data)]
    (with-open [file (io/writer path)]
      (csv/write-csv file (cons headers rows)))))

(defn read-header [^String file]
  (let [data (read-csv file)]
    (keys (first data))))
