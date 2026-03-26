# Security

## 1. Security Objectives

- Primary assets to protect: PII, restricted joinable identifiers, governance metadata, model artifacts, and trusted published datasets.
- Primary threats: exposure of unmasked sensitive data, unsafe joins across trust boundaries, unauthorized access to restricted datasets, and bypass of governance controls.
- Compliance drivers: auditable ownership, classification, masking, tokenization, certification, and access intent for published data assets.
- Abuse scenarios to prevent: broad-access Silver or Gold paths exposing raw sensitive values, direct access paths bypassing governed views or variants, and unstable tokenization that breaks approved joins or leaks identity.

## 2. Authentication

- Human authentication method: environment-appropriate local or cluster credentials are acceptable in v1; the spec does not require enterprise IAM for this repository.
- Service authentication method: service access is environment-specific, but trust boundaries must remain explicit between ingestion, processing, storage, metadata, and consumption layers.
- Token / credential format: implementation-specific for local mode.
- Credential rotation policy: credentials and secrets must remain separate from broad-access data paths and must be replaceable without altering dataset contracts.

## 3. Authorization

- Authorization model: access enforcement uses two layers, RBAC for coarse-grained role access and ABAC for context-aware restrictions on sensitive datasets.
- Role examples: `platform_admin`, `data_engineer`, `analyst`, and `ml_engineer`.
- Attribute examples: dataset classification, user team, environment, and approved purpose-of-use.
- Privileged actions: publishing or changing governance metadata, exposing restricted data paths, and altering masking or tokenization behavior.
- Break-glass policy: any temporary exception to normal access rules must be explicit, narrow in scope, and auditable.

## 4. Data Protection

### Classification and sensitivity
- Every persisted table and published view must declare one classification tag: `public`, `internal`, `confidential`, or `restricted`.
- Column-level tags apply to direct identifiers, quasi-identifiers, sensitive commercial data, and free text with potential PII.
- Certification tier: draft, candidate, certified, deprecated.
- Discoverability status: hidden, searchable, promoted.

### Data at Rest
- Encryption requirement: environment-specific, but stored sensitive data must remain governed and not rely solely on operator convention for protection.
- Key ownership: implementation-specific in local mode.

### Data in Transit
- Transport protection: environment-specific, especially for Kubernetes or service-to-service deployments.
- Internal service requirements: transport choices must not undermine the broader requirement that governed and restricted data paths remain separate from broad-access paths.

### Sensitive Data Handling
- PII / secrets / regulated data: customer, advertiser, and related identifiers and attributes that are marked sensitive by governance metadata.
- Restricted fields explicitly called out by the architecture: `customer.first_name`, `customer.last_name`, `customer.email`, `customer.phone`, `customer.zip_code`, and `sales_activity.notes`.
- Masking or redaction rules: PII must never appear unmasked in broad-access Silver or Gold views; free text such as sales notes must be masked or dropped in analyst-facing models.
- Tokenization rules: email, phone, and customer identifiers may require deterministic tokens for approved cross-table joins; tokens must be generated before broad-access publication and stored separately from raw values in restricted paths.
- Retention constraints: raw sensitive data should persist only where required by the architecture and governance rules; broad-access paths should expose masked or tokenized representations only.

## 5. Secrets and Key Management

- Secret storage mechanism: environment-specific local secrets or Kubernetes secrets are acceptable for v1.
- Access policy: least privilege by component, with no reason for BI or broad-access consumers to see secrets or raw restricted values.
- Rotation cadence: implementation-specific, but the system should not depend on hard-coded or non-rotatable secrets.
- Audit expectation: changes to security-relevant configuration and metadata should be reviewable in the repository and operational logs.

## 6. Security Monitoring and Response

- Security logs required: changes to masking, tokenization, ownership, access intent, classification, certification, and restricted publication paths.
- Detection signals: attempted publication of unmasked PII to broad-access datasets, missing governance metadata on published assets, and access-path violations for restricted data.
- Incident response owner: repository maintainers and platform operators for local or developer-cluster environments.
- Containment expectations: block publication of violating datasets, remove or restrict unsafe access paths, and rebuild governed outputs after corrective changes.

## 7. Residual Risks

1. The exact field-level mapping of which columns require masking versus deterministic tokenization still needs to be exhaustively enumerated.
2. Local and developer-cluster deployments may vary in transport and secret posture because enterprise IAM is intentionally out of scope.
3. BI and ad hoc SQL access patterns need continued review to ensure broad-access consumers cannot infer raw sensitive values from poorly designed derived datasets.
