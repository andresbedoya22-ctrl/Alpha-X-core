from __future__ import annotations

from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression

from alpha_x.modeling.base import ModelSpec


def get_model_specs() -> list[ModelSpec]:
    return [
        ModelSpec(
            model_id="logreg_l2",
            name="Logistic Regression L2",
            family="linear",
            description="Regularized logistic regression with class balancing.",
            parameters={
                "C": 0.5,
                "class_weight": "balanced",
                "max_iter": 2000,
                "solver": "lbfgs",
            },
            use_scaling=True,
            estimator=LogisticRegression(
                C=0.5,
                class_weight="balanced",
                max_iter=2000,
                solver="lbfgs",
                random_state=42,
            ),
        ),
        ModelSpec(
            model_id="random_forest_small",
            name="Random Forest Small",
            family="tree",
            description="Small random forest with shallow depth and conservative leaf sizes.",
            parameters={
                "n_estimators": 200,
                "max_depth": 4,
                "min_samples_leaf": 50,
                "class_weight": "balanced_subsample",
            },
            use_scaling=False,
            estimator=RandomForestClassifier(
                n_estimators=200,
                max_depth=4,
                min_samples_leaf=50,
                class_weight="balanced_subsample",
                random_state=42,
                n_jobs=1,
            ),
        ),
    ]
