# Immutable audit artifact

[Download the review archive](issue-resolution-review-2026-10-26.tar.gz) (47,675,190 bytes), then verify its [SHA-256](issue-resolution-review-2026-10-26.tar.gz.sha256). It preserves 46 source refs, 3,264 raw records and the evidence snapshot `6cc237fc8875d9426f975464e71c600445ab6a24`.

Every archived regular file was compared with its staged bytes. A separate [receipt audit](review-receipt-hash-audit-2026-10-26.json) verified 871 named file/hash pairs with no mismatches. Source SF1 tables, data archives, wheels and build caches are excluded; derived golden query answers are included.

The artifact is an append-only record on the separate evidence branch. It is linked by the prepared PR bodies and is not part of any product-code submission. No release or package publication is involved.

Submitted by David Hughes (@stugorf), with AI Collaboration.
