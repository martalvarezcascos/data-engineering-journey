import pandas as pd
import pytest

from src.transform import validate_orders

def test_validate_orders():
    df = pd.DataFrame(
        {
            "order_id": ["T9001"],
            "line_id": [1],
            "order_date": ["2026-09-20"],
            "customer_id": ["C_TEST"],
            "product_id": ["P001"],
            "quantity": [5],
            "unit_price": [10.0]
        }
    )

    with pytest.raises(ValueError):
        validate_orders(df)