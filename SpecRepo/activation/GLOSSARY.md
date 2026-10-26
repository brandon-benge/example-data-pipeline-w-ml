# Glossary

Acronyms are also spelled out on first use in each file. Terms marked with a star are explained in more depth in the file named.

| Term | Meaning |
|---|---|
| ABAC | Attribute-based access control: access granted by data tags and the user's attributes, not table by table |
| AEP | Adobe Experience Platform, Adobe's customer data platform |
| AUC | Area under the ROC curve, a ranking-quality metric for classifiers |
| CCPA / CPRA | California Consumer Privacy Act and its amendment, the California Privacy Rights Act |
| CDC | Change data capture: streaming row-level changes out of a source database |
| CI | Continuous integration: automated checks that run on every proposed change |
| CLV | Customer lifetime value: expected value of a person or household over a time horizon |
| CRM | Customer relationship management system, here Salesforce |
| ECID | Experience Cloud ID, Adobe's browser or device identifier |
| EDO | Enterprise Data Office |
| EPID | Enterprise person ID, the durable ID the identity spine issues (`enterprise_person_id`) |
| FAC | Federated Audience Composition: AEP audiences built from warehouse tables without copying the full data into AEP |
| GLBA | Gramm-Leach-Bliley Act, US financial privacy law |
| Marketing action | In AEP, the declared use of a destination, such as email targeting, onsite personalization or export to a third party. Policies say which data labels may not be used for which actions, and AEP blocks activation when they conflict |
| MDM | Master data management, the enterprise golden record for parties |
| ML | Machine learning |
| MMM ★ | Marketing mix model: estimates each channel's contribution to sales from weekly spend and outcomes, and where the next dollar should go ([05_ML.md](./05_ML.md), model 10) |
| MTA | Multi-touch attribution: splits credit for one person's conversion across the touches before it |
| NAIC | National Association of Insurance Commissioners |
| NBA ★ | Next best action: ranks every eligible offer or message for a person and picks one, using response odds, value, eligibility and fatigue ([05_ML.md](./05_ML.md), model 5) |
| NPN / CRD | National Producer Number (insurance producer licensing) / Central Registration Depository number (registered representatives) |
| PoC | Proof of concept |
| PSI | Population stability index: a number that measures how much a distribution (of scores or a feature) has shifted from the training baseline; used to detect drift |
| PQL | In AEP: Profile Query Language, the segment definition language. In the Benge Software file: product-qualified lead |
| RACI | Responsible, accountable, consulted, informed: a role assignment matrix |
| SCD type 2 | Slowly changing dimension type 2: history kept as effective-dated rows |
| SFMC | Salesforce Marketing Cloud |
| SLA / SLO | Service level agreement / objective: a promised freshness, quality or response time |
| Splink | An open-source Python library for probabilistic record linkage (matching records that refer to the same person without a shared key). It runs on Spark in Databricks, learns match weights from the data itself, and explains each match score field by field |
| Survivorship | Choosing which value survives into the golden record when several source records for one person disagree, by rules such as most trusted source, then most recent |
| TCPA | Telephone Consumer Protection Act: consent rules for calls and texts |
| Uplift ★ | The extra response caused by contacting someone, measured against a randomized holdout; separates persuadables from people who would buy anyway ([05_ML.md](./05_ML.md), model 6) |
| XDM | Experience Data Model, AEP's schema standard |
