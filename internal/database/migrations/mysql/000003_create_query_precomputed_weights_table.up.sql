CREATE TABLE QueryPrecomputedWeights (
    ID BIGINT UNSIGNED PRIMARY KEY,
    ComputationID BIGINT UNSIGNED NOT NULL,
    QueryFingerprintID BIGINT UNSIGNED NOT NULL,
    Weight TINYINT UNSIGNED NOT NULL,
    UNIQUE KEY uk_computation_id (ComputationID)
);
