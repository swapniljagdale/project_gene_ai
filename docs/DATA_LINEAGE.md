# Data Lineage Diagram

This document illustrates the data flow and dependencies between the tables across the different layers of the pipeline.

## Mermaid Diagram

```mermaid
graph TD
    subgraph Landing & Master Data
        A1[lnd_events]
        A2[lnd_participants]
        A3[master_product.csv]
        A4[master_indication.csv]
        A5[master_xref_participant.csv]
        A6[master_mdm_merge.csv]
    end

    subgraph Normalized Layer
        B1(normalized_lm_events)
        B2(normalized_lm_participants)
    end

    subgraph Summarized Layer
        C1{{event_performance_kpi}}
        C2{{product_performance_kpi}}
        C3{{participant_engagement_kpi}}
        C4{{event_type_participants_kpi}}
        C5{{brand_performance_kpi}}
    end

    subgraph Outbound Layer
        D1[/OUT_LM_EVENTS.csv/]
        D2[/OUT_LM_PARTICIPANTS.csv/]
    end

    %% --- Connections ---

    %% Landing to Normalized
    A1 --> B1
    A3 --> B1
    A4 --> B1
    A2 --> B2
    A5 --> B2
    A6 --> B2

    %% Normalized to Summarized
    B1 --> C1
    B2 --> C1
    B1 --> C2
    B2 --> C2
    B2 --> C3
    B1 --> C4
    B2 --> C4
    B1 --> C5
    B2 --> C5

    %% Normalized to Outbound
    B1 --> D1
    B2 --> D2
```

## Lineage Explanation

1.  **Landing to Normalized**:
    *   `normalized_lm_events` is created by enriching `lnd_events` with data from `master_product` and `master_indication`.
    *   `normalized_lm_participants` is created by enriching `lnd_participants` with master data from `master_xref_participant` and `master_mdm_merge` to get a consistent master ID.

2.  **Normalized to Summarized**:
    *   All five KPI tables in the **Summarized Layer** are derived from a join between the `normalized_lm_events` and `normalized_lm_participants` tables.
    *   The `participant_engagement_kpi` table is the only one that primarily uses the `normalized_lm_participants` table, but it is still conceptually linked to the overall event context.

3.  **Normalized to Outbound**:
    *   The outbound CSV files are direct, filtered exports from the two main tables in the **Normalized Layer**. `OUT_LM_EVENTS.csv` comes from `normalized_lm_events`, and `OUT_LM_PARTICIPANTS.csv` comes from `normalized_lm_participants`.
