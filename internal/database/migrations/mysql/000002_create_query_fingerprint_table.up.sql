CREATE TABLE QueryFingerprint (
    ID BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    Fingerprint TEXT NOT NULL,
    `Hash` BIGINT UNSIGNED NOT NULL, -- 128 bit hash,
    -- Count INT UNSIGNED NOT NULL DEFAULT 1, -- this will get incremented when the same QueryFingerprintHash exists instead of duplicating
    AVGExecutionTime BIGINT UNSIGNED,
    TimesCalled BIGINT UNSIGNED,
    AVGRowsScanned BIGINT UNSIGNED,
    AVGRowsReturned BIGINT UNSIGNED
);