-- CREATE INDEX idx__QueryFingerprint__Fingerprint ON QueryFingerprint (Fingerprint(625)); -- 2500 chars
CREATE INDEX idx__QueryFingerprint__Hash ON QueryFingerprint (Hash);
CREATE INDEX idx__Query_FingerprintHash ON Query (FingerprintHash);