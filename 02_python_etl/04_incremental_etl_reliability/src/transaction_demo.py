import psycopg
from database import DB_CONFIG


ORDER_ID = "O1001"
LINE_ID = 1


with psycopg.connect(**DB_CONFIG) as conn:
    with conn.cursor() as cur:

        # 1. Guardamos el estado original
        cur.execute(
            """
            SELECT quantity
            FROM core.orders
            WHERE order_id = %s
              AND line_id = %s;
            """,
            (ORDER_ID, LINE_ID),
        )

        original_quantity = cur.fetchone()[0]

        print(f"Original quantity: {original_quantity}")

        try:
            # 2. Operación válida
            cur.execute(
                """
                UPDATE core.orders
                SET quantity = %s
                WHERE order_id = %s
                  AND line_id = %s;
                """,
                (original_quantity + 10, ORDER_ID, LINE_ID),
            )

            print("Valid update executed.")

            # 3. Operación inválida deliberadamente
            cur.execute(
                """
                INSERT INTO core.orders (
                    order_id,
                    line_id,
                    order_date,
                    customer_id,
                    product_id,
                    quantity,
                    unit_price
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s);
                """,
                (
                    "O_TX_TEST",
                    1,
                    "2026-09-17",
                    "C999",
                    "P999",
                    -10,
                    20.00,
                ),
            )

            # Solo llegamos aquí si todo ha ido bien
            conn.commit()

        except psycopg.errors.CheckViolation as exc:
            print(f"Load failed: {exc}")
            print("Rolling back transaction...")

            conn.rollback()

        # 4. Comprobamos el estado DESPUÉS del rollback
        cur.execute(
            """
            SELECT quantity
            FROM core.orders
            WHERE order_id = %s
              AND line_id = %s;
            """,
            (ORDER_ID, LINE_ID),
        )

        final_quantity = cur.fetchone()[0]

        print(f"Final quantity: {final_quantity}")
        print(f"Rollback successful: {final_quantity == original_quantity}")