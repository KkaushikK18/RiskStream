flowchart TD
    subgraph P1["Phase 1: Data (done)"]
        A[PaySim CSV<br/>6.36M rows, read only] --> B[Adapter<br/>standard column names]
        S[Synthetic generator<br/>for tests and demos] --> B
        B --> C[Validation<br/>bad rows set aside with a reason]
        C --> D[EDA reports]
        C --> E[Split by time<br/>train 1-595 / val 596-669 / test 670-743]
    end
    subgraph P2["Phase 2: ML (done)"]
        E --> F[34 features<br/>from earlier hours only]
        F --> G[6 model types + Isolation Forest<br/>tuned on validation]
        G --> H[Calibration + cost-based thresholds]
        H --> I[Test set used once]
        I --> J[Model bundle + decision policy<br/>versioned files]
        G -.-> M[(MLflow)]
    end
    subgraph P3["Phase 3: Serving (done)"]
        J --> K[FastAPI service]
        PG[(PostgreSQL<br/>history, predictions, labels,<br/>outbox, audit)] <--> K
        K --> O[/APPROVE / REVIEW / DECLINE/]
    end
    subgraph LEFT["Phases 4-11 (not built)"]
        PG -.outbox.-> R[Redpanda streaming]
        R -.-> RD[(Redis online features)]
        RD -.-> K
        K -.-> MON[Prometheus + Grafana]
        PG -.labels.-> DR[Drift + delayed-label monitoring]
        DR -.-> RT[Prefect retraining]
        RT -.-> CC[Champion vs challenger]
        CC -.-> SC[Shadow / canary / rollback]
        SC -.-> K
    end
