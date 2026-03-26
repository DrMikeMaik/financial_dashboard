"""Gradio UI for the Financial Dashboard."""
from datetime import datetime

import gradio as gr

from app.services import account_service, bond_service, dashboard_service, holding_service, reference_service, transaction_service


ACCOUNT_TYPE_CHOICES = ["checking", "savings", "investment", "credit", "other"]
TRANSACTION_ACTION_CHOICES = ["buy", "sell", "dividend", "transfer"]
BOND_VALUATION_MODE_CHOICES = ["Unit price", "Percent of face"]


def _dashboard_payload(limit) -> tuple:
    limit = int(limit or 50)
    return dashboard_service.get_dashboard_payload(limit)


def _refresh_dashboard(limit) -> tuple:
    limit = int(limit or 50)
    return dashboard_service.refresh_and_get_dashboard(limit)


def _reference_updates() -> tuple:
    return (
        gr.update(choices=reference_service.list_transaction_choices(), value=None),
        gr.update(choices=reference_service.list_holding_symbols(), value=None),
        gr.update(choices=reference_service.list_account_names(), value=None),
        gr.update(choices=reference_service.list_account_choices(), value=None),
        gr.update(choices=reference_service.list_bond_choices(), value=None),
    )


def _load_bond_form(bond_choice: str | None) -> tuple:
    symbol, name, currency, face, coupon_rate, coupon_freq, maturity_date, issuer, status = bond_service.load_bond(bond_choice)
    valuation_info = bond_service.get_bond_valuation_details(bond_choice)
    return symbol, name, currency, face, coupon_rate, coupon_freq, maturity_date, issuer, status, valuation_info


def _clear_transaction_form() -> tuple:
    return (
        None,
        datetime.now().replace(microsecond=0).isoformat(sep=" "),
        None,
        None,
        "buy",
        None,
        None,
        0.0,
        "",
        "",
    )


def _clear_account_form() -> tuple:
    return None, "", None, "PLN", 0.0, True, ""


def _clear_bond_form() -> tuple:
    return None, "", "", "PLN", 100.0, 0.0, 1, "", "", "", "No bond selected."


def create_ui():
    """Create and configure the Gradio interface."""
    with gr.Blocks(title="Financial Dashboard") as demo:
        gr.Markdown("# 💰 Financial Dashboard")
        gr.Markdown("A local, PLN-first snapshot of your finances.")

        with gr.Row():
            refresh_btn = gr.Button("Refresh Prices", variant="primary", size="sm")
            refresh_output = gr.Textbox(label="Refresh Status", lines=4, interactive=False)

        with gr.Tabs():
            with gr.Tab("Overview"):
                summary_md = gr.Markdown()
                positions_df = gr.DataFrame(label="All Positions")

            with gr.Tab("Crypto"):
                crypto_df = gr.DataFrame(label="Crypto Holdings")

                gr.Markdown("### Add Crypto Holding")
                with gr.Row():
                    crypto_symbol = gr.Textbox(label="Symbol", scale=1)
                    crypto_name = gr.Textbox(label="Name", scale=2)
                    crypto_currency = gr.Textbox(label="Currency", value="USD", scale=1)

                crypto_save_btn = gr.Button("Save Crypto Holding")
                crypto_output = gr.Textbox(label="Crypto Result", interactive=False)

            with gr.Tab("Stocks & ETFs"):
                stocks_df = gr.DataFrame(label="Stock & ETF Holdings")

                gr.Markdown("### Add Stock / ETF Holding")
                with gr.Row():
                    stock_symbol = gr.Textbox(label="Symbol", scale=2)
                    stock_currency = gr.Textbox(label="Currency Override", value="USD", scale=1)

                stock_save_btn = gr.Button("Save Stock / ETF Holding")
                stock_output = gr.Textbox(label="Stock Result", interactive=False)

            with gr.Tab("Bonds"):
                bonds_df = gr.DataFrame(label="Bond Holdings")

                gr.Markdown("### Bond Editor")
                bond_select = gr.Dropdown(
                    label="Existing Bond",
                    choices=reference_service.list_bond_choices(),
                    value=None,
                )
                with gr.Row():
                    bond_symbol = gr.Textbox(label="Symbol", scale=1)
                    bond_name = gr.Textbox(label="Name", scale=2)
                    bond_currency = gr.Textbox(label="Currency", value="PLN", scale=1)

                with gr.Row():
                    bond_face = gr.Number(label="Face Value", value=100.0, scale=1)
                    bond_coupon_rate = gr.Number(label="Coupon Rate (%)", value=0.0, scale=1)
                    bond_coupon_freq = gr.Number(label="Coupons / Year", value=1, precision=0, scale=1)
                    bond_maturity = gr.Textbox(label="Maturity Date (YYYY-MM-DD)", scale=1)

                bond_issuer = gr.Textbox(label="Issuer")
                with gr.Row():
                    bond_save_btn = gr.Button("Save Bond", variant="primary")
                    bond_delete_btn = gr.Button("Delete Bond", variant="stop")
                    bond_clear_btn = gr.Button("Clear")

                bond_output = gr.Textbox(label="Bond Result", interactive=False)

                gr.Markdown("### Manual Valuation")
                with gr.Row():
                    bond_valuation_mode = gr.Dropdown(
                        label="Mode",
                        choices=BOND_VALUATION_MODE_CHOICES,
                        value="Unit price",
                        scale=1,
                    )
                    bond_valuation_value = gr.Number(label="Value", scale=1)
                    bond_valuation_ts = gr.Textbox(label="Timestamp (optional)", scale=2)

                bond_valuation_btn = gr.Button("Save Manual Valuation")
                bond_valuation_output = gr.Textbox(label="Valuation Result", interactive=False)
                bond_valuation_info = gr.Markdown("No bond selected.")

            with gr.Tab("Accounts"):
                accounts_df = gr.DataFrame(label="Accounts")

                gr.Markdown("### Account Editor")
                account_select = gr.Dropdown(
                    label="Existing Account",
                    choices=reference_service.list_account_choices(),
                    value=None,
                )
                with gr.Row():
                    acc_name = gr.Textbox(label="Name", scale=2)
                    acc_type = gr.Dropdown(label="Type", choices=ACCOUNT_TYPE_CHOICES, value=None, scale=1)
                    acc_currency = gr.Textbox(label="Currency", value="PLN", scale=1)
                    acc_balance = gr.Number(label="Balance", value=0.0, scale=1)
                acc_active = gr.Checkbox(label="Active", value=True)

                with gr.Row():
                    acc_save_btn = gr.Button("Save Account", variant="primary")
                    acc_delete_btn = gr.Button("Delete Account", variant="stop")
                    acc_clear_btn = gr.Button("Clear")

                acc_output = gr.Textbox(label="Account Result", interactive=False)

            with gr.Tab("Transactions"):
                txn_limit = gr.Slider(label="Show last N transactions", minimum=10, maximum=200, value=50, step=10)
                txn_df = gr.DataFrame(label="Recent Transactions")

                gr.Markdown("### Transaction Editor")
                tx_select = gr.Dropdown(
                    label="Existing Transaction",
                    choices=reference_service.list_transaction_choices(),
                    value=None,
                )
                with gr.Row():
                    txn_ts = gr.Textbox(
                        label="Timestamp",
                        value=datetime.now().replace(microsecond=0).isoformat(sep=" "),
                        scale=2,
                    )
                    txn_symbol = gr.Dropdown(
                        label="Holding Symbol",
                        choices=reference_service.list_holding_symbols(),
                        value=None,
                        scale=1,
                    )
                    txn_account = gr.Dropdown(
                        label="Account",
                        choices=reference_service.list_account_names(),
                        value=None,
                        scale=1,
                    )

                with gr.Row():
                    txn_action = gr.Dropdown(
                        label="Action",
                        choices=TRANSACTION_ACTION_CHOICES,
                        value="buy",
                        scale=1,
                    )
                    txn_qty = gr.Number(label="Quantity", scale=1)
                    txn_price = gr.Number(label="Price / Amount", scale=1)
                    txn_fee = gr.Number(label="Fee", value=0.0, scale=1)

                txn_note = gr.Textbox(label="Note")
                with gr.Row():
                    txn_save_btn = gr.Button("Save Transaction", variant="primary")
                    txn_delete_btn = gr.Button("Delete Transaction", variant="stop")
                    txn_clear_btn = gr.Button("Clear")

                txn_output = gr.Textbox(label="Transaction Result", interactive=False)

            with gr.Tab("Settings"):
                settings_md = gr.Markdown()
                gr.Markdown("*CSV export, import tooling, and ECB fallback remain deferred until the manual MVP is solid.*")

        dashboard_outputs = [
            refresh_output,
            summary_md,
            positions_df,
            crypto_df,
            stocks_df,
            bonds_df,
            accounts_df,
            txn_df,
            settings_md,
        ]

        refresh_reference_outputs = [
            tx_select,
            txn_symbol,
            txn_account,
            account_select,
            bond_select,
        ]

        refresh_btn.click(
            fn=_refresh_dashboard,
            inputs=txn_limit,
            outputs=dashboard_outputs,
        )

        demo.load(
            fn=_dashboard_payload,
            inputs=txn_limit,
            outputs=dashboard_outputs,
        )

        txn_limit.change(
            fn=_dashboard_payload,
            inputs=txn_limit,
            outputs=dashboard_outputs,
        )

        crypto_save_btn.click(
            fn=holding_service.add_crypto_holding,
            inputs=[crypto_symbol, crypto_name, crypto_currency],
            outputs=crypto_output,
        ).then(
            fn=_reference_updates,
            outputs=refresh_reference_outputs,
        ).then(
            fn=_dashboard_payload,
            inputs=txn_limit,
            outputs=dashboard_outputs,
        )

        stock_save_btn.click(
            fn=holding_service.add_stock_holding,
            inputs=[stock_symbol, stock_currency],
            outputs=stock_output,
        ).then(
            fn=_reference_updates,
            outputs=refresh_reference_outputs,
        ).then(
            fn=_dashboard_payload,
            inputs=txn_limit,
            outputs=dashboard_outputs,
        )

        bond_select.change(
            fn=_load_bond_form,
            inputs=bond_select,
            outputs=[
                bond_symbol,
                bond_name,
                bond_currency,
                bond_face,
                bond_coupon_rate,
                bond_coupon_freq,
                bond_maturity,
                bond_issuer,
                bond_output,
                bond_valuation_info,
            ],
        )

        bond_save_btn.click(
            fn=bond_service.save_bond,
            inputs=[
                bond_select,
                bond_symbol,
                bond_name,
                bond_currency,
                bond_face,
                bond_coupon_rate,
                bond_coupon_freq,
                bond_maturity,
                bond_issuer,
            ],
            outputs=bond_output,
        ).then(
            fn=_reference_updates,
            outputs=refresh_reference_outputs,
        ).then(
            fn=_dashboard_payload,
            inputs=txn_limit,
            outputs=dashboard_outputs,
        )

        bond_delete_btn.click(
            fn=bond_service.delete_bond,
            inputs=bond_select,
            outputs=bond_output,
        ).then(
            fn=_reference_updates,
            outputs=refresh_reference_outputs,
        ).then(
            fn=_dashboard_payload,
            inputs=txn_limit,
            outputs=dashboard_outputs,
        )

        bond_clear_btn.click(
            fn=_clear_bond_form,
            outputs=[
                bond_select,
                bond_symbol,
                bond_name,
                bond_currency,
                bond_face,
                bond_coupon_rate,
                bond_coupon_freq,
                bond_maturity,
                bond_issuer,
                bond_output,
                bond_valuation_info,
            ],
        )

        bond_valuation_btn.click(
            fn=bond_service.save_bond_valuation,
            inputs=[bond_select, bond_valuation_mode, bond_valuation_value, bond_valuation_ts],
            outputs=bond_valuation_output,
        ).then(
            fn=bond_service.get_bond_valuation_details,
            inputs=bond_select,
            outputs=bond_valuation_info,
        ).then(
            fn=_dashboard_payload,
            inputs=txn_limit,
            outputs=dashboard_outputs,
        )

        account_select.change(
            fn=account_service.load_account,
            inputs=account_select,
            outputs=[acc_name, acc_type, acc_currency, acc_balance, acc_active, acc_output],
        )

        acc_save_btn.click(
            fn=account_service.save_account,
            inputs=[account_select, acc_name, acc_type, acc_currency, acc_balance, acc_active],
            outputs=acc_output,
        ).then(
            fn=_reference_updates,
            outputs=refresh_reference_outputs,
        ).then(
            fn=_dashboard_payload,
            inputs=txn_limit,
            outputs=dashboard_outputs,
        )

        acc_delete_btn.click(
            fn=account_service.delete_account,
            inputs=account_select,
            outputs=acc_output,
        ).then(
            fn=_reference_updates,
            outputs=refresh_reference_outputs,
        ).then(
            fn=_dashboard_payload,
            inputs=txn_limit,
            outputs=dashboard_outputs,
        )

        acc_clear_btn.click(
            fn=_clear_account_form,
            outputs=[account_select, acc_name, acc_type, acc_currency, acc_balance, acc_active, acc_output],
        )

        tx_select.change(
            fn=transaction_service.load_transaction,
            inputs=tx_select,
            outputs=[txn_ts, txn_symbol, txn_account, txn_action, txn_qty, txn_price, txn_fee, txn_note, txn_output],
        )

        txn_save_btn.click(
            fn=transaction_service.save_transaction,
            inputs=[tx_select, txn_ts, txn_symbol, txn_account, txn_action, txn_qty, txn_price, txn_fee, txn_note],
            outputs=txn_output,
        ).then(
            fn=_reference_updates,
            outputs=refresh_reference_outputs,
        ).then(
            fn=_dashboard_payload,
            inputs=txn_limit,
            outputs=dashboard_outputs,
        )

        txn_delete_btn.click(
            fn=transaction_service.delete_transaction,
            inputs=tx_select,
            outputs=txn_output,
        ).then(
            fn=_reference_updates,
            outputs=refresh_reference_outputs,
        ).then(
            fn=_dashboard_payload,
            inputs=txn_limit,
            outputs=dashboard_outputs,
        )

        txn_clear_btn.click(
            fn=_clear_transaction_form,
            outputs=[tx_select, txn_ts, txn_symbol, txn_account, txn_action, txn_qty, txn_price, txn_fee, txn_note, txn_output],
        )

    return demo


def launch(share: bool = False, server_port: int = 7860):
    """Launch the Gradio UI."""
    demo = create_ui()
    demo.launch(
        share=share,
        server_port=server_port,
        server_name="0.0.0.0",
        theme=gr.themes.Soft(),
    )
