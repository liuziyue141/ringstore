# Acknowledgements and provenance

Ringstore extracts and refactors the storage and repair implementation from the UCSD CSE 223B Spring 2025 Lab 3 group project, `lab-3-group-2`. It omits the course assignment text, social frontend, grading interface, earlier lab implementations, and original repository history.

The source lab report credits Skyler Goh and Tim Liu. The course scaffold supplied the storage API, reference in-memory storage, escaping helpers, and protobuf storage schema. Those small supporting pieces are retained or adapted here and should not be presented as entirely original work by the showcase author.

The standalone extraction adds project structure, a CLI, diagnostics, a reproducible failure demo, build isolation, architecture notes, and CI. The core demonstrates consistent hashing, replicated operation histories, observed removal, restartable anti-entropy, and restart-aware clock allocation.

No new redistribution license is assigned by this extraction. Retained and adapted material remains subject to its original authors' rights and applicable permissions.
