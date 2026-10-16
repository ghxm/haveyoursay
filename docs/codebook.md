# haveyoursay datasets: codebook

Column tables at the end of this file are generated from `docs/codebook.csv`
by `scripts/build_codebook.py`; edit the CSV, then rerun the script.

## Datasets and keys

`haveyoursay dataset` exports one CSV per dataset, UTF-8, comma separated,
fields quoted where needed (standard double-quote doubling, no escape
character), missing values as empty fields, one header row. Rows are sorted
as listed. `manifest.json` next to the files records the tool version, the
database checksum, the collection runs and per-file row counts and
checksums; `SHA256SUMS` repeats the file checksums.

| Dataset | One row per | Sort order | Links |
|---|---|---|---|
| initiatives | initiative | id | |
| initiative_topics | initiative and topic (with `--long-tables`) | initiative_id, code | initiative_id to initiatives.id |
| initiative_policy_areas | initiative and policy area (with `--long-tables`) | initiative_id, code | initiative_id to initiatives.id |
| publications | initiative and publication pair, as served | initiative_id, id | initiative_id to initiatives.id |
| publication_attachments | Commission document (with `--attachments`) | publication_id, id | publication_id to publications.id |
| feedback | feedback item served by the portal | id | publication_id to publications.id |
| feedback_attachments | file attached to a feedback item (with `--attachments`) | feedback_id, id | feedback_id to feedback.id; publication_id to publications.id |
| attachments_text | downloaded file with extracted text (`dataset text`) | file order | id and type to the attachment datasets' id |

The portal nests publications inside initiatives and feedback inside
publications, but the relation between initiatives and publications is
many-to-many in practice: see the next section.

## Shared and colliding publication ids

The portal serves some publications under more than one initiative. A call
for evidence covering a package of initiatives appears in each member
initiative's record with the same `id` (the initiatives carry
`is_grouped_cfe`). The `publications` dataset keeps one row per
initiative-publication pair as served and marks these rows with
`id_shared = True`. For publication-level statistics (for example summing
`total_feedback`) deduplicate by `id`; when joining `feedback` on
`publication_id`, be aware that each item then matches once per sharing
initiative.

A small number of ids are served with different content under different
initiatives, typically a legacy open public consultation and an unrelated
later publication. These rows carry `id_collision = True`. Treat them as
distinct publications. The portal's feedback endpoint is keyed on the id
alone and serves the items of only one of the colliding publications; the
`feedback.reference_initiative` and `feedback.publication` columns carry
the labels the portal attaches to each item.

`group_id` is served by the portal but is rewritten per serving initiative;
it equals `initiative_id` in every row and carries no further information.

## Served items and reported totals

`feedback` contains the contributions the portal serves as individual
items. `publications.total_feedback` is the number of contributions the
Commission reports for the publication, which includes campaign and
questionnaire submissions that the portal aggregates. For campaign-heavy
consultations the two differ by orders of magnitude, and some closed public
consultations report a total but serve no items. A row count below
`total_feedback` is a property of the portal, not missing data.

## Dates, time zones, booleans, missing values

Portal dates are kept exactly as served: `YYYY/MM/DD HH:MM:SS` without a
time zone (the portal does not state one). The three tool timestamps
(`first_seen`, `last_seen`, `last_changed`) are UTC in
`YYYY-MM-DD HH:MM:SS`. Booleans are written as `True` and `False`, with an
empty field where the portal serves no value. Integers are written without a
decimal point; an empty field means the portal served no value. Empty text
and absent text are both written as empty fields.

## Timestamps and repeated runs

`first_seen` is the first run in which the tool stored the record.
`last_seen` is the last run in which the portal served it; after
`collect --refresh`, a record whose `last_seen` is older than the run
started is one the portal no longer serves. `last_changed` is the last run
in which the served record differed from the stored one; the stored record
is then overwritten.

## Three meanings of "stage" and "reference"

`initiatives.stage` is the initiative's internal workflow stage
(PLANNING_WORKFLOW, ISC_WORKFLOW, ADOPTION_WORKFLOW).
`publications.front_end_stage` is the stage label the portal shows for a
publication. Publication types (`publications.type`) are what analyses
usually group into lifecycle stages such as call for evidence, public
consultation, adopted proposal and draft act.

`initiatives.reference` is the Commission's reference for the initiative,
`publications.reference` the document reference of the publication (or the
survey name of a legacy consultation), `publication_attachments.reference`
the document reference of the file, and `feedback.reference_initiative` the
initiative reference the portal attaches to a feedback item.

## Public release (censored datasets)

`scripts/censor_datasets.py` writes a copy for public release that drops
the free text of contributions (`feedback.feedback`), submitter names
(`feedback.surname`, `feedback.first_name`) and submitter-chosen file names
and titles (`feedback_attachments.filename`, `feedback_attachments.title`),
blanks `feedback.organization` for citizen submissions (`user_type` of
EU_CITIZEN or NON_EU_CITIZEN), and never includes the raw `data` column.
Everything else, in particular all Commission-authored fields, is kept. The
`censored` column of the tables below marks the affected fields, and
`manifest.json` of a censored release records the rules applied. A full
collection, including the removed fields, can be reproduced with the tool.

## Vocabularies (August 2026 collection)

Values as served by the portal, including its spellings. New values can
appear in later collections.

initiatives.initiative_status: ACTIVE.

initiatives.stage: ADOPTION_WORKFLOW, PLANNING_WORKFLOW, ISC_WORKFLOW.

initiatives.receiving_feedback_status: CLOSED, UPCOMING, OPEN.
publications.receiving_feedback_status: DISABLED, CLOSED, UPCOMING, OPEN.

initiatives.foreseen_act_type (32 values): REG_IMPL, REG_DEL, PROP_REG,
COMMUNIC, REG, PROP_DIR, DEC_IMPL, SWD, REPORT, DIR_DEL, PROP_DEC, EVL,
DATPRO, PROP_RECO, RECO_DEC, ACT, JOINT_COMMUNIC, RECO, CORRIGENDUM, DEC_DEL,
DIR, DEC, DIR_IMPL, PROP_ACT, COMMUNIC_POSIT, PAPER_WHITE, JOINT_TEXT,
WORK_DOC, PAPER_GREEN, AMEND_PROP_REG, JOINT_REPORT, and empty.

publications.type (52 values): INIT_PLANNED, REG_IMPL, REG_IMPL_DRAFT,
REG_DEL, OPC_LAUNCHED, REG_DEL_DRAFT, PROP_REG, COMMUNIC, CALL_FOR_EVIDENCE,
IMPACT_ASSESS_INCEP, ROADMAP, PROP_DIR, REG, CFE_IMPACT_ASSESS, DEC_IMPL,
REG_DRAFT, SWD, ROADMAP_EFC, CFE_EVL_FC, DEC_IMPL_DRAFT, REPORT, PROP_DEC,
DIR_DEL, DIR_DEL_DRAFT, ABANDONED, EVL, PROP_RECO, ACT, RECO_DEC,
JOINT_COMMUNIC, CORRIGENDUM, RECO, DEC_DEL_DRAFT, DEC_DEL, DATPRO, DIR,
DIR_DRAFT, DEC, DIR_IMPL_DRAFT, DIR_IMPL, DEC_DRAFT, PROP_ACT, SUSPENDED,
COMMUNIC_POSIT, PAPER_WHITE, JOINT_TEXT, AMEND_PROP_REG, WORK_DOC,
PAPER_GREEN, PROP_REG_IMPL, JOINT_REPORT, AMEND_PROP_DIR.

publications.front_end_stage (8 values): ADOPTION_WORKFLOW, INIT_PLANNED,
ISC_WORKFLOW, ROADMAP, OPC_LAUNCHED, PLANNING_WORKFLOW, ABANDONED, SUSPENDED.

feedback.user_type (11 values and empty): EU_CITIZEN, COMPANY,
BUSINESS_ASSOCIATION, NON_EU_CITIZEN, NGO, OTHER, PUBLIC_AUTHORITY,
ACADEMIC_RESEARCH_INSTITTUTION, TRADE_UNION, ENVIRONMENTAL_ORGANISATION,
CONSUMER_ORGANISATION.

Topics (38): AGRI Agriculture and rural development; ASYL Migration and
asylum; BORDERS Borders and security; BUDGET Budget; BUSINESS Business and
industry; CLIMA Climate action; COMP Competition; CONSUM Consumers; CULT
Culture and media; CUSTOMS Customs; DIGITAL Digital economy and society; EAC
Education and training; ECFIN Economy, finance and the euro; EMPL Employment
and social affairs; ENER Energy; ENLARG EU enlargement; ENV Environment;
FINANCE Banking and financial services; FOOD Food safety; FOREIGN Foreign
affairs and security policy; FRAUD Fraud prevention; HEALTH Public health;
HOME Home affairs; HUMAN Humanitarian aid and civil protection; INST
Institutional affairs; INTDEV International cooperation and development; JUST
Justice and fundamental rights; MARE Maritime affairs and fisheries;
NEIGHBOUR European neighbourhood policy; REGIO Regional policy; RESEARCH
Research and innovation; SINGMARK Single market; SPORT Sport; STAT
Statistics; TAX Taxation; TRADE Trade; TRANSPORT Transport; YOUTH Youth.

Policy areas (79 codes, Commission-internal, labels partly duplicated across
codes): exported as served in `initiative_policy_areas`.

## Column tables

### initiatives

| Column | Source | Type | Format | Censored | Description |
|---|---|---|---|---|---|
| id | initiatives table | integer |  |  | Initiative identifier assigned by the portal (the number in the portal URL). |
| reference | $.reference | text |  |  | Commission reference of the initiative (Ares, PLAN or COM number). Not unique across initiatives. |
| unit | $.unit | text |  |  | Lead Commission unit. |
| dg | $.dg | text |  |  | Lead Directorate-General (acronym). |
| committee | $.committee | text |  |  | Comitology committee named by the Commission, if any. |
| expert_group | $.expertGroup | text |  |  | Expert group named by the Commission, if any. |
| dossier_summary | $.dossierSummary | text |  |  | Commission summary of the initiative. |
| short_title | $.shortTitle | text |  |  | Title shown on the portal, as served (may contain line breaks and surrounding spaces). |
| created_date | $.createdDate | text | YYYY/MM/DD HH:MM:SS (no time zone) |  | Creation of the portal record. |
| published_date | $.publishedDate | text | YYYY/MM/DD HH:MM:SS (no time zone) |  | Publication on the portal. |
| modified_date | $.modifiedDate | text | YYYY/MM/DD HH:MM:SS (no time zone) |  | Last modification of the portal record. |
| initiative_status | $.initiativeStatus | text | vocabulary |  | Record status. ACTIVE for every initiative in the August 2026 collection. |
| foreseen_act_type | $.foreseenActType | text | vocabulary |  | Type of act the Commission foresees. Empty where the portal serves none. |
| receiving_feedback_status | $.receivingFeedbackStatus | text | CLOSED, UPCOMING, OPEN |  | Whether a feedback window of the initiative is open at collection. |
| stage | $.stage | text | PLANNING_WORKFLOW, ISC_WORKFLOW, ADOPTION_WORKFLOW |  | Internal workflow stage of the initiative. Distinct from publications.front_end_stage. |
| is_major | $.isMajor | boolean | True, False, empty |  | Flag set by the Commission. Empty where the portal serves no value. |
| is_evaluation | $.isEvaluation | boolean | True, False, empty |  | Initiative is an evaluation. Empty where the portal serves no value. |
| is_grouped_cfe | $.isGroupedCfe | boolean | True, False, empty |  | Initiative belongs to a package that shares one call for evidence (see publications.id_shared). |
| topics | $.topics[*].label | text | labels joined with '; ' |  | Commission topic classification (long form: initiative_topics). |
| topic_codes | $.topics[*].code | text | codes joined with '; ' in the same order as topics |  | Topic codes. |
| policy_areas | $.policyAreas[*].label | text | labels joined with '; ' |  | Commission policy area classification (long form: initiative_policy_areas). |
| policy_area_codes | $.policyAreas[*].code | text | codes joined with '; ' in the same order as policy_areas |  | Policy area codes. |
| fetch_failed | tool | boolean | True, False |  | True when the initiative appeared in the portal search but its record could not be retrieved; the other columns are then empty. |
| first_seen | tool | text | YYYY-MM-DD HH:MM:SS (UTC) |  | First collection run that saw the initiative. |
| last_seen | tool | text | YYYY-MM-DD HH:MM:SS (UTC) |  | Last collection run in which the portal served the initiative. |
| last_changed | tool | text | YYYY-MM-DD HH:MM:SS (UTC) |  | Last collection run in which the served record differed from the stored one. |
| data | $ | text | JSON | never in the public release | Raw portal response (--include-data only). Contains contact details of Commission staff. |

### initiative_topics

| Column | Source | Type | Format | Censored | Description |
|---|---|---|---|---|---|
| initiative_id | initiatives table | integer |  |  | Initiative identifier. |
| code | $.topics[*].code | text | vocabulary |  | Topic code. |
| label | $.topics[*].label | text | vocabulary |  | Topic label. |

### initiative_policy_areas

| Column | Source | Type | Format | Censored | Description |
|---|---|---|---|---|---|
| initiative_id | initiatives table | integer |  |  | Initiative identifier. |
| code | $.policyAreas[*].code | text | vocabulary |  | Policy area code. |
| label | $.policyAreas[*].label | text | vocabulary |  | Policy area label. |

### publications

| Column | Source | Type | Format | Censored | Description |
|---|---|---|---|---|---|
| id | $.publications[*].id | integer |  |  | Publication identifier. Not unique on its own: see id_shared and id_collision. Rows are unique by (initiative_id, id). |
| initiative_id | initiatives table | integer |  |  | Initiative under which the publication was served. |
| type | $.publications[*].type | text | vocabulary |  | Publication type, i.e. the kind of document or step the publication represents. |
| receiving_feedback_status | $.publications[*].receivingFeedbackStatus | text | DISABLED, CLOSED, UPCOMING, OPEN |  | Feedback window status at collection. DISABLED: the publication never offered a feedback window. |
| reference | $.publications[*].reference | text |  |  | Document reference (Ares, COM or C number) or, for legacy open public consultations, the survey name. |
| reference_number | $.publications[*].referenceNumber | text |  |  | Interinstitutional or internal reference number (e.g. PLAN/2024/1, 2025/0010(COD), ISC/2016/03034). |
| group_id | $.publications[*].groupId | integer |  |  | Served by the portal but rewritten per serving initiative: equals initiative_id in every row and carries no further information. |
| title | $.publications[*].title | text |  |  | Publication title as served (may contain line breaks and surrounding spaces). |
| front_end_stage | $.publications[*].frontEndStage | text | vocabulary |  | Stage label the portal shows for this publication. |
| total_feedback | $.publications[*].totalFeedback | integer |  |  | Number of contributions the Commission reports for the publication, including campaign and questionnaire submissions the portal does not serve as individual items. Compare with the count of rows in feedback. |
| feedback_period | $.publications[*].feedbackPeriod | integer | weeks |  | Length of the feedback window in weeks as served; 0 where none was offered. |
| published_date | $.publications[*].publishedDate | text | YYYY/MM/DD HH:MM:SS (no time zone) |  | Publication date. Empty for a sizable share of rows. |
| created_date | $.publications[*].createdDate | text | YYYY/MM/DD HH:MM:SS (no time zone) |  | Creation of the portal record. |
| modified_date | $.publications[*].modifiedDate | text | YYYY/MM/DD HH:MM:SS (no time zone) |  | Last modification of the portal record. |
| adoption_date | $.publications[*].adoptionDate | text | YYYY/MM/DD HH:MM:SS (no time zone) |  | Adoption date for adopted acts and proposals. |
| end_date | $.publications[*].endDate | text | YYYY/MM/DD HH:MM:SS (no time zone) |  | End of the feedback window. |
| id_shared | tool | boolean | True, False |  | True when the same publication (same type, reference, reference number and title) is served under more than one initiative. Deduplicate by id for publication-level statistics. |
| id_collision | tool | boolean | True, False |  | True when the portal serves different publications under the same id. Treat the rows as distinct publications. The feedback endpoint keys on id and serves the items of only one of them. |
| data | $.publications[*] | text | JSON | never in the public release | Raw publication object (--include-data only). Contains contact details of Commission staff. |

### publication_attachments

| Column | Source | Type | Format | Censored | Description |
|---|---|---|---|---|---|
| id | $.publications[*].attachments[*].id | integer |  |  | Attachment identifier. |
| document_id | $.publications[*].attachments[*].documentId | text |  |  | Identifier used by the portal's download endpoint. |
| reference | $.publications[*].attachments[*].reference | text |  |  | Document reference. |
| type | $.publications[*].attachments[*].type | text |  |  | Document type code. |
| work_type | $.publications[*].attachments[*].workType | text |  |  | Document work type code. |
| category | $.publications[*].attachments[*].category | text |  |  | Document category (main text, annex, etc.). |
| title | $.publications[*].attachments[*].title | text |  |  | Document title. |
| publication_type | publications.type | text | vocabulary |  | Type of the publication the attachment belongs to. |
| date | $.publications[*].attachments[*].date | text | YYYY/MM/DD HH:MM:SS (no time zone) |  | Document date. |
| created_date | $.publications[*].attachments[*].createdDate | text | YYYY/MM/DD HH:MM:SS (no time zone) |  | Creation of the portal record. |
| modified_date | $.publications[*].attachments[*].modifiedDate | text | YYYY/MM/DD HH:MM:SS (no time zone) |  | Last modification of the portal record. |
| filename | $.publications[*].attachments[*].ersFileName or .filename | text |  |  | File name on the portal (Commission document). |
| language | $.publications[*].attachments[*].language | text | language code as served |  | Document language. |
| pages | $.publications[*].attachments[*].pages | integer |  |  | Page count as served. |
| size | $.publications[*].attachments[*].size | integer | bytes |  | File size as served. |
| is_original | $.publications[*].attachments[*].isOriginal | boolean | True, False, empty |  | Original language version. |
| published | $.publications[*].attachments[*].published | boolean | True, False, empty |  | Published flag as served. |
| publication_id | publications.id | integer |  |  | Publication the attachment belongs to. |
| data | $.publications[*].attachments[*] | text | JSON | never in the public release | Raw attachment object (--include-data only). |

### feedback

| Column | Source | Type | Format | Censored | Description |
|---|---|---|---|---|---|
| id | feedback table | integer |  |  | Feedback item identifier. |
| publication_id | feedback table | integer |  |  | Publication whose feedback endpoint served the item. |
| tr_number | $.tr_number | text |  |  | EU Transparency Register number declared by the submitter. |
| language | $.language | text | language code as served |  | Language of the contribution. |
| country | $.country | text | country code as served |  | Country declared by the submitter. |
| organization | $.organization | text |  | blanked for EU_CITIZEN and NON_EU_CITIZEN rows in the public release | Organization name declared by the submitter. |
| surname | $.surname | text |  | dropped in the public release | Submitter surname. |
| first_name | $.firstName | text |  | dropped in the public release | Submitter first name. |
| status | $.status | text | as served |  | Item status. |
| publication_status | $.publicationStatus | text | as served |  | Publication status of the item. |
| feedback | $.feedback | text |  | dropped in the public release | Free text of the contribution. |
| date_feedback | $.dateFeedback | text | YYYY/MM/DD HH:MM:SS (no time zone) |  | Submission time as served. |
| publication | $.publication | text |  |  | Publication label served with the item. Not documented by the portal. |
| user_type | $.userType | text | vocabulary |  | Self-declared actor type, including the portal's spelling ACADEMIC_RESEARCH_INSTITTUTION. Empty where no type was declared. |
| company_size | $.companySize | text | as served |  | Company size declared by company submitters. |
| reference_initiative | $.referenceInitiative | text |  |  | Initiative reference served with the item. |
| first_seen | tool | text | YYYY-MM-DD HH:MM:SS (UTC) |  | First collection run that saw the item. |
| last_seen | tool | text | YYYY-MM-DD HH:MM:SS (UTC) |  | Last collection run in which the portal served the item. |
| last_changed | tool | text | YYYY-MM-DD HH:MM:SS (UTC) |  | Last collection run in which the served item differed from the stored one. |
| data | $ | text | JSON | never in the public release | Raw feedback object (--include-data only). |

### feedback_attachments

| Column | Source | Type | Format | Censored | Description |
|---|---|---|---|---|---|
| id | $.attachments[*].id | integer |  |  | Attachment identifier. |
| document_id | $.attachments[*].documentId | text |  |  | Identifier used by the portal's download endpoint. |
| filename | $.attachments[*].ersFileName | text |  | dropped in the public release | File name chosen by the submitter. |
| title | $.attachments[*].title | text |  | dropped in the public release | Title chosen by the submitter. |
| category | $.attachments[*].category | text |  |  | Attachment category as served. |
| pages | $.attachments[*].pages | integer |  |  | Page count as served. |
| size | $.attachments[*].size | integer | bytes |  | File size as served. |
| feedback_language | feedback.language | text | language code as served |  | Language of the feedback item. |
| feedback_status | feedback.status | text | as served |  | Status of the feedback item. |
| feedback_id | feedback.id | integer |  |  | Feedback item the attachment belongs to. |
| publication_id | feedback.publication_id | integer |  |  | Publication of the feedback item. |
| data | $.attachments[*] | text | JSON | never in the public release | Raw attachment object (--include-data only). |
