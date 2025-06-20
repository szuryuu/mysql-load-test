ALTER TABLE Query
DROP FOREIGN KEY fk_query_fingerprint;

ALTER TABLE QueryPrecomputedWeights
DROP FOREIGN KEY fk_precomputed_weights_fingerprint;

DROP TABLE IF EXISTS QueryPrecomputedWeights;