(ns csv-utils.cbind
  "Merge several csv files by a column."
  (:require [csv-utils.lib :refer [read-csv vec-remove write-csv-by-cols! read-header]]
            [clojure.java.io :as io]))

(set! *warn-on-reflection* true)

(defn- sort-by-val [s]        (sort-by val s))
(defn- first-elements [pairs] (map #(get % 0) pairs))

(defn most-frequent-n
  "return the most common n items, e.g. 
     (most-frequent-n 2 [:a :b :a :d :x :b :c :d :d :b :d :b])  => 
         => (:d :b)"
  [n items]
  (take n (->
           items               ; [:a :b :a :d :x :b :c :d :d :b :d :b]
           frequencies         ; {:a 2, :b 4, :d 4, :x 1, :c 1}
           seq                 ; ([:a 2] [:b 4] [:d 4] [:x 1] [:c 1])
           sort-by-val         ; ([:x 1] [:c 1] [:a 2] [:b 4] [:d 4])
           reverse             ; ([:d 4] [:b 4] [:a 2] [:c 1] [:x 1])
           first-elements)))   ; (:d :b :a :c :x)

(defn guess-id
  [files]
  (->> (map read-header files)  ; [{:GENE_ID :SAMPLE1} {:GENE_ID :SAMPLE2}]
       (flatten)                ; [:GENE_ID :SAMPLE1 :GENE_ID :SAMPLE2]
       (most-frequent-n 1)      ; [:GENE_ID]
       (first)))                ; :GENE_ID

(def id ^:private (atom :GENE_ID))

(defn setup-id!
  [^clojure.lang.Keyword v]
  (reset! id v))

(defn sort-data
  [coll]
  (sort-by @id coll))

(defn read-csvs
  [files]
  (map #(sort-data (read-csv %)) files))

(defn indices-of [f coll]
  (keep-indexed #(if (f %2) %1 nil) coll))

(defn first-index-of [f coll]
  (first (indices-of f coll)))

(defn find-index [coll id]
  (first-index-of #(= % id) coll))

(defn reorder
  [data]
  (let [cols (vec (sort (keys (first data))))]
    (cons @id (vec-remove (find-index cols @id) cols))))

(defn merge-data
  "Examples:
   [[{:GENE_ID 'XXX0' :YYY0 1.2} {:GENE_ID 'XXX1' :YYY1 1.3}]
    [{:GENE_ID 'XXX0' :YYY2 1.2} {:GENE_ID 'XXX1' :YYY3 1.3}]]"
  [all-data]
  (apply map merge all-data))

(defn write-csv-by-ordered-cols!
  [path row-data]
  (let [cols (reorder row-data)]
    (write-csv-by-cols! path row-data cols)))

(defn copy-file [^String source-path ^String dest-path]
  (io/copy (io/file source-path) (io/file dest-path)))

(defn merge-files!
  "Merge several csv files to a single file by the same column.

   Assumption:
     all files have the same id column, no matter what order."
  [files path]
  (if (= (count files) 1)
    (copy-file (first files) path)
    (let [id (guess-id (take 2 files))]
      (setup-id! id)
      (->> (read-csvs files)
           (merge-data)
           (write-csv-by-ordered-cols! path)))))
