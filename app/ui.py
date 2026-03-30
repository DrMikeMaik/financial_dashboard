"""Gradio UI for the Financial Dashboard."""
from datetime import datetime

import gradio as gr

from app.services import account_service, bond_service, dashboard_service, holding_service, reference_service, transaction_service


ACCOUNT_TYPE_CHOICES = ["checking", "savings", "investment", "credit", "other"]
TRANSACTION_ACTION_CHOICES = ["buy", "sell", "dividend", "transfer"]


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


def _append_bond_rate_from_choice(bond_choice: str | None, rate) -> str:
    if not bond_choice or not str(bond_choice).strip():
        return "✗ Select a bond."

    try:
        bond_id = int(str(bond_choice).split("|", 1)[0].strip())
    except ValueError:
        return "✗ Invalid bond selection."

    return bond_service.append_bond_rate(bond_id, rate)




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
                bond_ids_state = gr.State([])
                bonds_df = gr.DataFrame(
                    label="Bonds",
                    wrap=True,
                    line_breaks=True,
                    datatype=["str", "number", "str", "str", "str", "markdown", "str", "str", "str"],
                )

                with gr.Row():
                    bond_series = gr.Textbox(label="Series (e.g. COI0528)", scale=2)
                    bond_qty = gr.Number(label="Qty", precision=0, minimum=1, scale=1)
                    bond_date = gr.DateTime(label="Purchase Date", include_time=False, type="datetime", scale=1)
                    bond_rate = gr.Number(label="Year 1 Rate (%)", minimum=0, precision=2, step=0.01, scale=1)
                    bond_add_btn = gr.Button("Add Bond", variant="primary", scale=1)

                with gr.Row():
                    bond_select = gr.Dropdown(
                        label="Existing Bond",
                        choices=reference_service.list_bond_choices(),
                        allow_custom_value=False,
                        value=None,
                        scale=3,
                    )
                    bond_next_rate = gr.Number(label="Next Year Rate (%)", minimum=0, precision=2, step=0.01, scale=1)
                    bond_append_btn = gr.Button("Append Next Rate", scale=1)

                bond_output = gr.Textbox(label="Result", interactive=False)

            with gr.Tab("Accounts"):
                accounts_df = gr.DataFrame(label="Accounts")

                gr.Markdown("### Account Editor")
                account_select = gr.Dropdown(
                    label="Existing Account",
                    choices=reference_service.list_account_choices(),
                    allow_custom_value=True, value=None,
                )
                with gr.Row():
                    acc_name = gr.Textbox(label="Name", scale=2)
                    acc_type = gr.Dropdown(label="Type", choices=ACCOUNT_TYPE_CHOICES, allow_custom_value=True, value=None, scale=1)
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
                    allow_custom_value=True, value=None,
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
                        allow_custom_value=True, value=None,
                        scale=1,
                    )
                    txn_account = gr.Dropdown(
                        label="Account",
                        choices=reference_service.list_account_names(),
                        allow_custom_value=True, value=None,
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
            bond_ids_state,
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

        bond_add_btn.click(
            fn=bond_service.add_bond,
            inputs=[bond_series, bond_qty, bond_date, bond_rate],
            outputs=bond_output,
        ).then(
            fn=_reference_updates,
            outputs=refresh_reference_outputs,
        ).then(
            fn=_dashboard_payload,
            inputs=txn_limit,
            outputs=dashboard_outputs,
        )

        bond_append_btn.click(
            fn=_append_bond_rate_from_choice,
            inputs=[bond_select, bond_next_rate],
            outputs=bond_output,
        ).then(
            fn=_reference_updates,
            outputs=refresh_reference_outputs,
        ).then(
            fn=_dashboard_payload,
            inputs=txn_limit,
            outputs=dashboard_outputs,
        )

        def _handle_bond_table_click(evt: gr.SelectData, bond_ids):
            if evt.value != "🗑️":
                return gr.skip()
            row = evt.index[0]
            if row >= len(bond_ids):
                return gr.skip()
            return bond_service.delete_bond_by_id(bond_ids[row])

        bonds_df.select(
            fn=_handle_bond_table_click,
            inputs=bond_ids_state,
            outputs=bond_output,
        ).then(
            fn=_reference_updates,
            outputs=refresh_reference_outputs,
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
