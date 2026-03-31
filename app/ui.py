"""Gradio UI for the Financial Dashboard."""
from datetime import datetime

import gradio as gr

from app.services import (
    account_service,
    bond_service,
    crypto_ledger_service,
    dashboard_service,
    reference_service,
    stock_ledger_service,
    transaction_service,
)


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
        gr.update(choices=crypto_ledger_service.list_crypto_order_choices(), value=None),
        gr.update(choices=stock_ledger_service.list_stock_order_choices(), value=None),
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


def _clear_stock_form() -> tuple:
    return (
        gr.update(value=None),
        "",
        [],
        gr.update(choices=[], value=None),
        "",
        "",
        "",
        "",
        datetime.now().replace(hour=0, minute=0, second=0, microsecond=0),
        "buy",
        None,
        None,
        0.0,
        "",
        "",
    )


def _clear_crypto_form() -> tuple:
    return (
        gr.update(value=None),
        "",
        [],
        gr.update(choices=[], value=None),
        "",
        "PLN",
        "",
        "",
        datetime.now().replace(hour=0, minute=0, second=0, microsecond=0),
        "buy",
        None,
        None,
        0.0,
        "",
        "",
    )


def _search_crypto_candidates(query: str | None) -> tuple:
    results, message = crypto_ledger_service.search_crypto_candidates(query or "")
    choices = [result["label"] for result in results]

    if not results:
        return (
            gr.update(value=None),
            [],
            gr.update(choices=[], value=None),
            "",
            "PLN",
            "",
            "",
            message,
        )

    selected_choice = choices[0] if len(choices) == 1 else None
    selected = crypto_ledger_service.resolve_search_choice(selected_choice, results)
    return (
        gr.update(value=None),
        results,
        gr.update(choices=choices, value=selected_choice),
        selected["symbol"] if selected else "",
        "PLN",
        selected["name"] if selected else "",
        selected["id"] if selected else "",
        message,
    )


def _apply_crypto_search_choice(selected_choice: str | None, results_state) -> tuple:
    selected = crypto_ledger_service.resolve_search_choice(selected_choice, results_state)
    if selected is None:
        return "", "PLN", "", "", ""
    return (
        selected["symbol"],
        "PLN",
        selected["name"] or "",
        selected["id"] or "",
        f"Selected {selected['symbol']}.",
    )


def _load_crypto_order_form(order_choice: str | None) -> tuple:
    payload = crypto_ledger_service.load_crypto_order(order_choice)
    choices = [result["label"] for result in payload["results_state"]]
    return (
        payload["search_query"],
        payload["results_state"],
        gr.update(choices=choices, value=payload["selected_choice"]),
        payload["resolved_symbol"],
        payload["trade_currency"],
        payload["asset_name"],
        payload["coingecko_id"],
        payload["timestamp_text"],
        payload["action"],
        payload["quantity"],
        payload["price"],
        payload["fee"],
        payload["note"],
        payload["message"],
    )


def _save_crypto_order_and_refresh(
    limit,
    order_choice: str | None,
    selected_search_choice: str | None,
    results_state,
    timestamp_text,
    action,
    quantity,
    price,
    fee_pln,
    note: str,
) -> tuple:
    result = crypto_ledger_service.save_crypto_order(
        order_choice,
        selected_search_choice,
        results_state,
        timestamp_text,
        action,
        quantity,
        price,
        fee_pln,
        note,
    )
    reference_updates = _reference_updates()
    dashboard_payload = _refresh_dashboard(limit) if result.startswith("✓") else _dashboard_payload(limit)
    return (result, *reference_updates, *dashboard_payload)


def _search_stock_candidates(query: str | None) -> tuple:
    results, message = stock_ledger_service.search_stock_candidates(query or "")
    choices = [result["label"] for result in results]

    if not results:
        return (
            gr.update(value=None),
            [],
            gr.update(choices=[], value=None),
            "",
            "",
            "",
            "",
            message,
        )

    selected_choice = choices[0] if len(choices) == 1 else None
    selected = stock_ledger_service.resolve_search_choice(selected_choice, results)
    return (
        gr.update(value=None),
        results,
        gr.update(choices=choices, value=selected_choice),
        selected["symbol"] if selected else "",
        selected["currency"] if selected else "",
        selected["name"] if selected else "",
        selected["exchange_label"] if selected else "",
        message,
    )


def _apply_stock_search_choice(selected_choice: str | None, results_state) -> tuple:
    selected = stock_ledger_service.resolve_search_choice(selected_choice, results_state)
    if selected is None:
        return "", "", "", "", ""
    return (
        selected["symbol"],
        selected["currency"] or "",
        selected["name"] or "",
        selected["exchange_label"] or "",
        f"Selected {selected['symbol']}.",
    )


def _load_stock_order_form(order_choice: str | None) -> tuple:
    payload = stock_ledger_service.load_stock_order(order_choice)
    choices = [result["label"] for result in payload["results_state"]]
    return (
        payload["search_query"],
        payload["results_state"],
        gr.update(choices=choices, value=payload["selected_choice"]),
        payload["resolved_symbol"],
        payload["trade_currency"],
        payload["security_name"],
        payload["exchange_label"],
        payload["timestamp_text"],
        payload["action"],
        payload["quantity"],
        payload["price"],
        payload["commission"],
        payload["note"],
        payload["message"],
    )


def _save_stock_order_and_refresh(
    limit,
    order_choice: str | None,
    selected_search_choice: str | None,
    results_state,
    timestamp_text,
    action,
    quantity,
    price,
    commission_pln,
    note: str,
) -> tuple:
    result = stock_ledger_service.save_stock_order(
        order_choice,
        selected_search_choice,
        results_state,
        timestamp_text,
        action,
        quantity,
        price,
        commission_pln,
        note,
    )
    reference_updates = _reference_updates()
    dashboard_payload = _refresh_dashboard(limit) if result.startswith("✓") else _dashboard_payload(limit)
    return (result, *reference_updates, *dashboard_payload)


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
                crypto_order_ids_state = gr.State([])
                crypto_search_results_state = gr.State([])
                crypto_df = gr.DataFrame(
                    label="Crypto Orders",
                    wrap=True,
                    line_breaks=True,
                    datatype=["str", "markdown", "str", "str", "str", "str", "str", "str", "str", "str", "str"],
                )

                gr.Markdown("### Crypto Order Editor")
                crypto_order_select = gr.Dropdown(
                    label="Existing Crypto Order",
                    choices=crypto_ledger_service.list_crypto_order_choices(),
                    allow_custom_value=False,
                    value=None,
                )

                with gr.Row():
                    crypto_search_query = gr.Textbox(label="Search CoinGecko", placeholder="e.g. BTC or bitcoin", scale=3)
                    crypto_search_btn = gr.Button("Search", scale=1)

                crypto_result_select = gr.Dropdown(
                    label="Search Results",
                    choices=[],
                    allow_custom_value=False,
                    value=None,
                )

                with gr.Row():
                    crypto_resolved_symbol = gr.Textbox(label="Resolved Symbol", interactive=False, scale=1)
                    crypto_trade_currency = gr.Textbox(label="Trade Currency", value="PLN", interactive=False, scale=1)
                    crypto_asset_name = gr.Textbox(label="Asset Name", interactive=False, scale=2)
                    crypto_coingecko_id = gr.Textbox(label="CoinGecko ID", interactive=False, scale=2)

                with gr.Row():
                    crypto_ts = gr.DateTime(
                        label="Date",
                        value=datetime.now().replace(hour=0, minute=0, second=0, microsecond=0),
                        include_time=False,
                        type="datetime",
                        scale=2,
                    )
                    crypto_action = gr.Dropdown(label="Action", choices=["buy", "sell"], value="buy", scale=1)

                with gr.Row():
                    crypto_qty = gr.Number(label="Qty", scale=1)
                    crypto_price = gr.Number(label="Spot Price (PLN)", scale=1)
                    crypto_fee = gr.Number(label="Fee (PLN)", value=0.0, scale=1)

                crypto_note = gr.Textbox(label="Note")
                with gr.Row():
                    crypto_save_btn = gr.Button("Save Crypto Order", variant="primary")
                    crypto_clear_btn = gr.Button("Clear")
                crypto_output = gr.Textbox(label="Crypto Result", interactive=False)

            with gr.Tab("Stocks & ETFs"):
                stock_order_ids_state = gr.State([])
                stocks_df = gr.DataFrame(
                    label="Stock & ETF Orders",
                    wrap=True,
                    line_breaks=True,
                    datatype=["str", "markdown", "str", "str", "str", "str", "str", "str", "str", "str", "str", "str"],
                )

                gr.Markdown("### Stock / ETF Order Editor")
                stock_search_results_state = gr.State([])
                stock_order_select = gr.Dropdown(
                    label="Existing Stock / ETF Order",
                    choices=stock_ledger_service.list_stock_order_choices(),
                    allow_custom_value=False,
                    value=None,
                )

                with gr.Row():
                    stock_search_query = gr.Textbox(label="Search Yahoo Finance", placeholder="e.g. EUNM or iShares MSCI Emerging Markets", scale=3)
                    stock_search_btn = gr.Button("Search", scale=1)

                stock_result_select = gr.Dropdown(
                    label="Search Results",
                    choices=[],
                    allow_custom_value=False,
                    value=None,
                )

                with gr.Row():
                    stock_resolved_symbol = gr.Textbox(label="Resolved Yahoo Symbol", interactive=False, scale=1)
                    stock_trade_currency = gr.Textbox(label="Trade Currency", interactive=False, scale=1)
                    stock_security_name = gr.Textbox(label="Security Name", interactive=False, scale=2)
                    stock_exchange_label = gr.Textbox(label="Exchange", interactive=False, scale=2)

                with gr.Row():
                    stock_ts = gr.DateTime(
                        label="Date",
                        value=datetime.now().replace(hour=0, minute=0, second=0, microsecond=0),
                        include_time=False,
                        type="datetime",
                        scale=2,
                    )
                    stock_action = gr.Dropdown(label="Action", choices=["buy", "sell"], value="buy", scale=1)

                with gr.Row():
                    stock_qty = gr.Number(label="Qty", scale=1)
                    stock_price = gr.Number(label="Price", scale=1)
                    stock_fee = gr.Number(label="Commission (PLN)", value=0.0, scale=1)

                stock_note = gr.Textbox(label="Note")
                with gr.Row():
                    stock_save_btn = gr.Button("Save Stock / ETF Order", variant="primary")
                    stock_clear_btn = gr.Button("Clear")
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
            crypto_order_ids_state,
            stocks_df,
            stock_order_ids_state,
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
            crypto_order_select,
            stock_order_select,
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

        crypto_order_select.change(
            fn=_load_crypto_order_form,
            inputs=crypto_order_select,
            outputs=[
                crypto_search_query,
                crypto_search_results_state,
                crypto_result_select,
                crypto_resolved_symbol,
                crypto_trade_currency,
                crypto_asset_name,
                crypto_coingecko_id,
                crypto_ts,
                crypto_action,
                crypto_qty,
                crypto_price,
                crypto_fee,
                crypto_note,
                crypto_output,
            ],
        )

        crypto_search_btn.click(
            fn=_search_crypto_candidates,
            inputs=crypto_search_query,
            outputs=[
                crypto_order_select,
                crypto_search_results_state,
                crypto_result_select,
                crypto_resolved_symbol,
                crypto_trade_currency,
                crypto_asset_name,
                crypto_coingecko_id,
                crypto_output,
            ],
        )

        crypto_result_select.change(
            fn=_apply_crypto_search_choice,
            inputs=[crypto_result_select, crypto_search_results_state],
            outputs=[
                crypto_resolved_symbol,
                crypto_trade_currency,
                crypto_asset_name,
                crypto_coingecko_id,
                crypto_output,
            ],
        )

        crypto_save_btn.click(
            fn=_save_crypto_order_and_refresh,
            inputs=[txn_limit, crypto_order_select, crypto_result_select, crypto_search_results_state, crypto_ts, crypto_action, crypto_qty, crypto_price, crypto_fee, crypto_note],
            outputs=[crypto_output, *refresh_reference_outputs, *dashboard_outputs],
        )

        def _handle_crypto_table_click(evt: gr.SelectData, crypto_order_ids):
            if evt.value != "🗑️":
                return gr.skip()
            row = evt.index[0]
            if row >= len(crypto_order_ids):
                return gr.skip()
            return crypto_ledger_service.delete_crypto_order_by_id(crypto_order_ids[row])

        crypto_df.select(
            fn=_handle_crypto_table_click,
            inputs=crypto_order_ids_state,
            outputs=crypto_output,
        ).then(
            fn=_reference_updates,
            outputs=refresh_reference_outputs,
        ).then(
            fn=_dashboard_payload,
            inputs=txn_limit,
            outputs=dashboard_outputs,
        )

        crypto_clear_btn.click(
            fn=_clear_crypto_form,
            outputs=[
                crypto_order_select,
                crypto_search_query,
                crypto_search_results_state,
                crypto_result_select,
                crypto_resolved_symbol,
                crypto_trade_currency,
                crypto_asset_name,
                crypto_coingecko_id,
                crypto_ts,
                crypto_action,
                crypto_qty,
                crypto_price,
                crypto_fee,
                crypto_note,
                crypto_output,
            ],
        )

        stock_order_select.change(
            fn=_load_stock_order_form,
            inputs=stock_order_select,
            outputs=[
                stock_search_query,
                stock_search_results_state,
                stock_result_select,
                stock_resolved_symbol,
                stock_trade_currency,
                stock_security_name,
                stock_exchange_label,
                stock_ts,
                stock_action,
                stock_qty,
                stock_price,
                stock_fee,
                stock_note,
                stock_output,
            ],
        )

        stock_search_btn.click(
            fn=_search_stock_candidates,
            inputs=stock_search_query,
            outputs=[
                stock_order_select,
                stock_search_results_state,
                stock_result_select,
                stock_resolved_symbol,
                stock_trade_currency,
                stock_security_name,
                stock_exchange_label,
                stock_output,
            ],
        )

        stock_result_select.change(
            fn=_apply_stock_search_choice,
            inputs=[stock_result_select, stock_search_results_state],
            outputs=[
                stock_resolved_symbol,
                stock_trade_currency,
                stock_security_name,
                stock_exchange_label,
                stock_output,
            ],
        )

        stock_save_btn.click(
            fn=_save_stock_order_and_refresh,
            inputs=[txn_limit, stock_order_select, stock_result_select, stock_search_results_state, stock_ts, stock_action, stock_qty, stock_price, stock_fee, stock_note],
            outputs=[stock_output, *refresh_reference_outputs, *dashboard_outputs],
        )

        def _handle_stock_table_click(evt: gr.SelectData, stock_order_ids):
            if evt.value != "🗑️":
                return gr.skip()
            row = evt.index[0]
            if row >= len(stock_order_ids):
                return gr.skip()
            return stock_ledger_service.delete_stock_order_by_id(stock_order_ids[row])

        stocks_df.select(
            fn=_handle_stock_table_click,
            inputs=stock_order_ids_state,
            outputs=stock_output,
        ).then(
            fn=_reference_updates,
            outputs=refresh_reference_outputs,
        ).then(
            fn=_dashboard_payload,
            inputs=txn_limit,
            outputs=dashboard_outputs,
        )

        stock_clear_btn.click(
            fn=_clear_stock_form,
            outputs=[
                stock_order_select,
                stock_search_query,
                stock_search_results_state,
                stock_result_select,
                stock_resolved_symbol,
                stock_trade_currency,
                stock_security_name,
                stock_exchange_label,
                stock_ts,
                stock_action,
                stock_qty,
                stock_price,
                stock_fee,
                stock_note,
                stock_output,
            ],
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
