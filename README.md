## GridGate Queue Withdrawal and Drop-Round Prediction
The project creates two predictive models using data from the "GI_Interactive_Queue.csv" file, which is based on interconnection queue data, using features created by "Database_Engineer.SQL".
The workflow involves loading raw data from a queue into an in-memory SQLite database, followed by a series of SQL-based data preprocessing and feature engineering steps, and then building a logistic regression model through Scikit-learn.

### Model 1 — Withdrawal Predictor (Binary Classification)
Predicts whether a project will:

- **Withdraw** from the queue (`1`)
- **Not Withdraw** / remain active or complete (`0`)

### Model 2 — Drop Round Predictor (Multiclass Classification)
Predicts **which stage a project exits the queue**, or whether it completes.

Classes:

- `0` = Completes / Does Not Drop
- `1` = Study Not Started
- `2` = Phase 1
- `3` = Phase 2
- `4` = Phase 3
- `5` = GIA


## Folder Structure

```bash
project_folder/
│
├── model1_withdrawal.py
├── model2_drop_round.py
│
├── Datasets/
│   ├── Database_Engineer.SQL
│   └── GI_Interactive_Queue.csv
│
└── README.md
