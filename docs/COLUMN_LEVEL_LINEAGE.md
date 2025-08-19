# Column-Level Data Lineage

This document provides a detailed, column-level view of the data lineage, showing how individual fields are transformed and populated across the pipeline's layers. For clarity, the lineage is broken down by each major target table.

---

## 1. Lineage for `normalized_lm_events`

This diagram shows how the `normalized_lm_events` table is constructed from the landing layer event table and enriched with master data.

```mermaid
graph TD
    subgraph Sources
        A[lnd_events<br>event_id<br>event_name<br>event_date<br>event_brand<br>indication_name]
        B[product_master<br>source_product_name<br>brand_id<br>brand_name]
        C[indication_master<br>source_indication_name<br>indication_id]
    end

    subgraph Transformation Logic
        T1{Join on<br>event_brand = source_product_name}
        T2{Join on<br>indication_name = source_indication_name}
    end

    subgraph Target
        D(normalized_lm_events<br>event_id<br>event_name<br>event_date<br>brand_id<br>brand_name<br>indication_id)
    end

    %% Direct Mappings
    A -- event_id --> D
    A -- event_name --> D
    A -- event_date --> D

    %% Join for Brand Info
    A -- event_brand --> T1
    B -- source_product_name --> T1
    T1 -- brand_id --> D
    T1 -- brand_name --> D

    %% Join for Indication Info
    A -- indication_name --> T2
    C -- source_indication_name --> T2
    T2 -- indication_id --> D
```

---

## 2. Lineage for `normalized_lm_participants`

This diagram illustrates how the `normalized_lm_participants` table is created, focusing on the logic to resolve the master participant ID (`final_mdm_id`).

```mermaid
graph TD
    subgraph Sources
        A[lnd_participants<br>participant_id<br>event_id<br>attended]
        B[master_xref_participant<br>source_id<br>mdm_id]
        C[master_mdm_merge<br>loser_mdm_id<br>winner_mdm_id]
    end

    subgraph Transformation Logic
        T1{Join on<br>participant_id = source_id}
        T2{Left Join on<br>mdm_id = loser_mdm_id}
        T3{COALESCE<br>final_mdm_id =<br>winner_mdm_id OR mdm_id}
    end

    subgraph Target
        D(normalized_lm_participants<br>participant_id<br>event_id<br>attended<br>final_mdm_id)
    end

    %% Direct Mappings
    A -- participant_id --> D
    A -- event_id --> D
    A -- attended --> D

    %% MDM ID Resolution
    A -- participant_id --> T1
    B -- source_id --> T1
    T1 -- mdm_id --> T2
    C -- loser_mdm_id --> T2
    T2 -- mdm_id, winner_mdm_id --> T3
    T3 -- final_mdm_id --> D
```

---

## 3. Lineage for `event_performance_kpi` (Summarized)

This diagram shows how columns from the two normalized tables are aggregated to produce the `event_performance_kpi` table.

```mermaid
graph TD
    subgraph Sources (Normalized Layer)
        A[normalized_lm_events<br>event_id<br>event_date]
        B[normalized_lm_participants<br>participant_id<br>attended<br>event_id]
    end

    subgraph Transformation Logic
        T1{Join on event_id}
        T2{Group By event_date}
        T3{AGGREGATIONS<br>COUNT_DISTINCT(event_id)<br>COUNT(participant_id)<br>SUM(attended)}
    end

    subgraph Target (Summarized Layer)
        C{{event_performance_kpi<br>event_date<br>total_events<br>total_registrations<br>total_attendees}}
    end

    %% Join & Grouping
    A -- event_id --> T1
    B -- event_id --> T1
    A -- event_date --> T1
    T1 -- grouped by event_date --> T2
    T2 -- event_date --> C

    %% Aggregations
    T1 -- event_id --> T3
    T3 -- total_events --> C
    T1 -- participant_id --> T3
    T3 -- total_registrations --> C
    T1 -- attended --> T3
    T3 -- total_attendees --> C
```
