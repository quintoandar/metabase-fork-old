(ns metabase.query-processor.middleware.resolve-fields
  "Middleware that resolves the Fields referenced by a query."
  (:require [metabase.mbql.util :as mbql.u]
            [metabase.models.field :refer [Field]]
            [metabase.query-processor.store :as qp.store]
            [metabase.util :as u]
            [toucan.db :as db]))

(def ^:private columns-to-fetch
  "Columns to fetch and stash in the QP store, and return as part of the `:cols` metadata in query results. Try to keep
  this set pared down to just what's needed by the QP and frontend, since it has to be done for every MBQL query."
  [:base_type
   :database_type
   :description
   :display_name
   :fingerprint
   #_:fk_target_field_id
   :id
   :name
   :parent_id
   #_:settings
   :special_type
   :table_id
   :visibility_type])

(defn- resolve-fields* [{mbql-inner-query :query, :as query}]
  (u/prog1 query
    (when-let [field-ids (seq (map second (mbql.u/clause-instances :field-id mbql-inner-query)))]
      (doseq [field (db/select (vec (cons Field columns-to-fetch)) :id [:in (set field-ids)])]
        (qp.store/store-field! field)))))

(defn resolve-fields
  "Fetch the Fields referenced by `:field-id` clauses in a query and store them in the Query Processor Store for the
  duration of the Query Execution."
  [qp]
  (comp qp resolve-fields*))
