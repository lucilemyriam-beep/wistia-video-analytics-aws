# Requirements and evidence

| ID | Requirement | Evidence / status |
|---|---|---|
| FR1 | Architecture | Draw.io source and screenshot included; SME approval reported |
| FR2 | Authentication | Lambda uses Bearer; PDF text specifies Basic Auth, while its sample uses Bearer. Confirm with SME |
| FR3 | Metadata | Two media rows verified |
| FR4 | Engagement metrics | API extraction reported; validate persisted records |
| FR5 | Visitor data | 101,289 visitor dimension rows verified; verify IP/events coverage |
| FR6 | Pagination | Inspect deployed Lambda implementation and logs |
| FR7 | Incremental ingestion | Inspect deployed Lambda checkpoint logic and evidence |
| FR8 | Seven production days | Evidence not included; must be verified |
| FR9 | CI/CD | Validation workflow included; deployment workflow not verified |
| FR10 | Structured DWH | 2 media rows, 101,289 visitor rows; fact tables 0, COPY schema mismatch unresolved |
| FR11 | Optional dashboard | Saved Redshift chart screenshot included |
| FR12 | GitHub deliverables | Starter structure prepared; complete deployed source, evidence, setup, and walkthrough before final submission |
