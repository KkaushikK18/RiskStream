# RiskStream

**Real-Time Fraud Detection and Risk Scoring Platform**

A production-oriented traditional Machine Learning platform demonstrating end-to-end ML engineering capabilities for fraud detection in financial transactions.

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## 📋 Project Overview

RiskStream is a comprehensive ML engineering project that goes beyond simple model training to demonstrate:

- **Machine Learning Engineering**: Feature engineering, model experimentation, evaluation, and calibration
- **Backend Engineering**: REST API, real-time inference, streaming, caching, and distributed systems
- **MLOps**: Experiment tracking, model monitoring, automated retraining, and safe deployment

This is **not** a simple ML demo. It's a complete, production-style platform built to demonstrate practical ML engineering skills for AI/ML Engineer and Applied ML Engineer positions.

### Business Problem

Financial services companies process thousands of transactions and need to evaluate each one for fraud risk in real-time. RiskStream simulates this environment by:

1. Receiving incoming transactions
2. Computing real-time behavioral features
3. Scoring transactions with ML models
4. Making risk-based decisions (APPROVE/REVIEW/DECLINE)
5. Monitoring model performance and data drift
6. Triggering automated retraining when needed
7. Safely deploying improved models

---

## 🎯 Project Goals

This project demonstrates three core engineering dimensions:

### A. Machine Learning Engineering
- Advanced feature engineering with temporal correctness
- Multiple model experimentation (Logistic Regression, Random Forest, XGBoost, LightGBM)
- Proper evaluation for imbalanced classification
- Threshold optimization and model calibration
- Feature consistency between training and inference

### B. Backend & Distributed Systems
- FastAPI REST API for real-time inference
- Redis for online feature store
- Apache Kafka (Redpanda) for event streaming
- PostgreSQL for transaction persistence
- Horizontal scaling and load balancing
- Failure handling and resilience

### C. MLOps & Production Engineering
- MLflow for experiment tracking and model registry
- Evidently for drift and performance monitoring
- Prefect for orchestration
- Champion/challenger evaluation
- Shadow and canary deployments
- Comprehensive observability with Prometheus and Grafana

---

## 🏗️ Architecture

```
┌─────────────────┐
│   Transaction   │
│     Source      │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────────────┐
│          FastAPI Inference Service          │
│  ┌─────────────┐  ┌──────────────────────┐ │
│  │   Feature   │  │   ML Model (LightGBM,│ │
│  │  Retrieval  │─▶│   XGBoost, etc.)     │ │
│  │  (Redis)    │  │                      │ │
│  └─────────────┘  └──────────────────────┘ │
└──────────┬──────────────────────────────────┘
           │
           ▼
    ┌─────────────┐
    │  PostgreSQL │
    │ (Predictions│
    │  & Labels)  │
    └─────────────┘
           │
           ▼
    ┌─────────────┐      ┌──────────────┐
    │   Kafka/    │─────▶│   Streaming  │
    │  Redpanda   │      │   Consumers  │
    └─────────────┘      └──────┬───────┘
                                │
                         ┌──────▼───────┐
                         │    Redis     │
                         │   Feature    │
                         │    Update    │
                         └──────────────┘
```

---

## 📊 Dataset

### Primary Dataset: PaySim

PaySim is a synthetic financial transaction dataset simulating mobile money transactions.

- **Source**: [Kaggle - PaySim1](https://www.kaggle.com/datasets/ealaxi/paysim1)
- **Size**: ~6.3M transactions over 30 days (743 hourly steps)
- **Features**: Transaction type, amount, origin/destination balances, fraud labels
- **Characteristics**: Highly imbalanced (~0.13% fraud rate)

**Important**: This dataset is synthetic and used for research/demonstration purposes only. Results do not represent validated performance on real financial transactions.

### Synthetic Data

For testing and development without downloading the full dataset, RiskStream includes a deterministic synthetic transaction generator.

---

## 🚀 Phase 1: Foundation & Data Pipeline (CURRENT)

### Status: ✅ Complete

Phase 1 establishes the data foundation:

- ✅ Project structure and dependency management
- ✅ Configuration management
- ✅ Structured logging
- ✅ Synthetic transaction generator
- ✅ PaySim dataset ingestion and validation
- ✅ Exploratory Data Analysis (EDA)
- ✅ Chronological train/val/test splitting
- ✅ Unit tests

---

## 📦 Installation

### Prerequisites

- Python 3.11+ (tested with Python 3.14.3)
- Git
- Docker (for later phases)

### Setup

1. **Clone the repository**:
```bash
git clone <repository-url>
cd RiskStream
```

2. **Create and activate virtual environment**:
```bash
python -m venv venv

# Windows
venv\Scripts\activate

# macOS/Linux
source venv/bin/activate
```

3. **Install dependencies**:
```bash
pip install -r requirements.txt
```

4. **Create environment file**:
```bash
# Windows
copy .env.example .env

# macOS/Linux
cp .env.example .env
```

---

## 🎯 Quick Start

### Option 1: Use PaySim Dataset (Recommended)

1. **Download PaySim dataset** from [Kaggle](https://www.kaggle.com/datasets/ealaxi/paysim1)

2. **Place the CSV** in `data/raw/paysim/PS_20174392719_1491204439457_log.csv`

3. **Run data preparation pipeline**:
```bash
python scripts/prepare_data.py
```

This will:
- Load and validate the PaySim dataset
- Generate comprehensive EDA reports
- Create chronological train/val/test splits
- Save processed data to `data/processed/`

### Option 2: Generate Synthetic Demo Data

For quick testing without downloading the full dataset:

```bash
python scripts/generate_demo_data.py --num-transactions 10000 --run-eda
```

### Verify Installation

Run tests to ensure everything is working:

```bash
pytest tests/ -v
```

---

## 📁 Project Structure

```
RiskStream/
│
├── README.md                 # This file
├── pyproject.toml           # Project metadata and dependencies
├── requirements.txt         # Python dependencies
├── .env.example             # Environment template
├── Makefile                 # Convenient commands
│
├── configs/                 # Configuration files
│   ├── logging.yaml
│   └── data.yaml
│
├── data/                    # Data directory (gitignored)
│   ├── raw/                 # Original datasets
│   ├── interim/             # Validated data
│   ├── processed/           # Train/val/test splits
│   └── synthetic/           # Generated demo data
│
├── src/riskstream/          # Main source code
│   ├── config.py            # Configuration management
│   ├── logging_config.py    # Logging setup
│   │
│   └── data/                # Data pipeline
│       ├── schemas.py       # Data schemas
│       ├── validation.py    # Validation logic
│       ├── ingestion.py     # Data loading
│       ├── synthetic_generator.py  # Synthetic data
│       ├── eda.py           # Exploratory analysis
│       └── preprocessing.py # Splitting and preprocessing
│
├── scripts/                 # Executable scripts
│   ├── prepare_data.py      # Data preparation pipeline
│   └── generate_demo_data.py  # Synthetic data generation
│
├── tests/                   # Test suite
│   └── unit/                # Unit tests
│
└── reports/                 # Generated reports (gitignored)
    └── eda/                 # EDA visualizations and stats
```

---

## 🧪 Data Pipeline

### Validation

The data pipeline includes comprehensive validation:
- Schema compliance
- Missing value analysis
- Data type checking
- Categorical value validation
- Numeric range validation
- Duplicate detection
- Target distribution analysis

### Chronological Splitting

**Critical for preventing temporal leakage:**

- **Train set**: Steps 1-595 (~80%)
- **Validation set**: Steps 596-669 (~10%)
- **Test set**: Steps 670-743 (~10%)

The test set represents future transactions the model has never seen during training or validation.

### Exploratory Data Analysis

The EDA pipeline generates:
- Basic statistics and data quality metrics
- Target distribution analysis
- Transaction type distributions
- Amount distributions
- Fraud patterns by transaction type
- Temporal transaction patterns
- Visualizations (PNG charts)
- JSON report with all metrics

View results in `reports/eda/`.

---

## 🧮 Key Statistics (PaySim)

- **Total Transactions**: 6,362,620
- **Fraud Rate**: ~0.13% (highly imbalanced)
- **Imbalance Ratio**: ~770:1 (legitimate:fraudulent)
- **Transaction Types**: CASH_OUT, PAYMENT, CASH_IN, TRANSFER, DEBIT
- **Highest Fraud Rate**: TRANSFER transactions
- **Temporal Span**: 743 hourly steps (~30 days)

---

## 🔬 Testing

```bash
# Run all tests
pytest tests/ -v

# Run unit tests only
pytest tests/unit/ -v

# Run with coverage
pytest tests/ -v --cov=src/riskstream --cov-report=html

# View coverage report
# Open htmlcov/index.html in browser
```

---

## 📈 Development Roadmap

### ✅ Phase 1: Foundation & Data Pipeline (COMPLETE)
- Project setup and configuration
- Data ingestion and validation
- EDA pipeline
- Chronological splitting
- Synthetic data generation

### 🚧 Phase 2: ML Experimentation (NEXT)
- Baseline models (Logistic Regression, Decision Tree)
- Advanced models (Random Forest, XGBoost, LightGBM)
- Feature engineering
- Hyperparameter tuning
- Threshold optimization
- Model calibration
- MLflow experiment tracking

### ⏳ Phase 3: Inference Backend
- FastAPI application
- Model loading and serving
- Request/response schemas
- Health and readiness endpoints
- PostgreSQL integration

### ⏳ Phase 4: Online Features & Streaming
- Redis feature store
- Kafka/Redpanda integration
- Event producers and consumers
- Transaction replay

### ⏳ Phase 5: Local Production Environment
- Docker Compose orchestration
- Service configuration
- Persistent volumes
- Health checks

### ⏳ Phase 6: Infrastructure Observability
- Prometheus metrics
- Grafana dashboards
- Structured logging
- Alert rules

### ⏳ Phase 7: Model Monitoring & Retraining
- Evidently drift detection
- Performance monitoring
- Prefect workflows
- Champion/challenger evaluation

### ⏳ Phase 8: Safe Model Deployment
- Shadow predictions
- Canary deployment
- Model rollback

### ⏳ Phase 9: Scalability & Resilience
- Horizontal scaling
- Load testing with Locust
- Failure injection
- Performance optimization

### ⏳ Phase 10: AWS Deployment
- Terraform infrastructure
- ECS Fargate services
- Managed databases and caching
- CloudWatch monitoring

### ⏳ Phase 11: CI/CD & Final Hardening
- GitHub Actions
- Automated tests
- Security checks
- Documentation

---

## 🛠️ Technology Stack

### Core ML & Data
- Python 3.11+
- Polars (fast data processing)
- Pandas (ML integration)
- Scikit-learn
- XGBoost
- LightGBM

### Backend (Future Phases)
- FastAPI
- PostgreSQL
- Redis
- Apache Kafka (Redpanda)

### MLOps (Future Phases)
- MLflow
- Evidently
- Prefect

### Infrastructure (Future Phases)
- Docker
- Docker Compose
- Terraform
- AWS (ECR, ECS, RDS, ElastiCache)

### Monitoring (Future Phases)
- Prometheus
- Grafana
- CloudWatch

---

## 📝 Key Engineering Decisions

### Why Polars?
Polars provides significantly better performance and memory efficiency than Pandas for the ~6M row PaySim dataset, especially during CSV loading and Parquet conversion.

### Why Chronological Splitting?
PaySim has temporal structure (hourly steps). Random splitting would allow the model to learn from "future" transactions, creating unrealistic performance estimates.

### Why Separate Train/Val/Test?
- **Train**: Model fitting and learning
- **Validation**: Hyperparameter tuning and threshold selection
- **Test**: Final unbiased evaluation (never touched during development)

### Why Schema Validation?
Explicit schemas catch data quality issues early and document expected structure for maintenance and collaboration.

---

## 🚨 Important Limitations

1. **Synthetic Data**: PaySim is synthetic. Results are for demonstration only.
2. **No Real PII**: Never contains real customer or financial data.
3. **Research Purpose**: Not validated for actual production fraud detection.
4. **Simplified Features**: Real fraud detection uses far more sophisticated features.
5. **No Regulatory Compliance**: Doesn't address financial regulations or compliance requirements.

---

## 🤝 Contributing

This is a portfolio project for demonstrating ML engineering capabilities. While it's not open for external contributions, the architecture and decisions are documented for learning and reference.

---

## 📚 References

- PaySim Dataset: [Kaggle](https://www.kaggle.com/datasets/ealaxi/paysim1)
- Original Paper: Lopez-Rojas, Edgar Alonso; Axelsson, Stefan (2016)

---

## 📄 License

MIT License - See LICENSE file for details.

---

## 👤 Author

**Kaushik** - Computer Science Undergraduate  
Building practical ML engineering skills for AI/ML Engineer roles

---

## 🎓 Learning Outcomes

This project demonstrates:

✅ End-to-end ML pipeline design  
✅ Production code quality and testing  
✅ Data validation and temporal correctness  
✅ Configuration-driven development  
✅ Reproducible experiments  
✅ Clear documentation and communication  

**Next**: Move to Phase 2 for model development and experimentation.

---

**Status**: Phase 1 Complete | Last Updated: 2026-11-15
