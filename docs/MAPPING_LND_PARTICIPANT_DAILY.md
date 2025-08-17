# Source-to-Target Mapping: preprocess_lnd_participant_daily.py

This document outlines the source-to-target mapping for the `preprocess_lnd_participant_daily.py` script, which is responsible for populating the daily landing table for participants.

## 1. Process Overview

The script implements an incremental loading pattern. Its primary goal is to update the participant landing table with new or changed records from the daily preprocess feed while preserving the existing historical data.

**The core logic is as follows:**
1.  **Read Sources**: It reads two data sources:
    *   The daily preprocessed participant data.
    *   The existing full snapshot of the participant landing table.
2.  **Identify New Records**: It performs a `left_anti` join to filter out any records from the daily preprocess data that already exist in the snapshot, based on the composite key (`participant_id`, `event_id`).
3.  **Combine Data**: It unions the new/unique records from the preprocess feed with the full snapshot data to create the new, updated landing table.

## 2. Data Flow Diagram

```mermaid
graph TD
    subgraph Sources
        A[Preprocess Data<br>(Daily Feed)]
        B[Landing Snapshot<br>(Existing Data)]
    end

    subgraph Transformation
        T1{LEFT ANTI JOIN<br>on [participant_id, event_id]<br>to find new records}
        T2{UNION}
    end

    subgraph Target
        C[New Landing Table<br>(Updated Snapshot)]
    end

    A --> T1
    B --> T1
    T1 -- New Records --> T2
    B -- Existing Records --> T2
    T2 --> C
```

## 3. Column Mapping

Since the operation is a `unionByName`, the column mapping is **1-to-1**. The schema of the source (preprocess and snapshot) is expected to be identical to the target schema. The script does not perform any transformations on the column values themselves.

| Source Column | Target Column | Transformation Logic |
| :--- | :--- | :--- |
| `participant_id` | `participant_id` | Direct mapping. Used as part of the key for the anti-join. |
| `event_id` | `event_id` | Direct mapping. Used as part of the key for the anti-join. |
| `hcp_id` | `hcp_id` | Direct mapping. |
| `... (all other columns)` | `... (all other columns)` | Direct mapping via `unionByName`. |
