
import asyncio
import logging
import traceback
from datetime import datetime
import base64
import json

import polars as pl 
import signal
import sys
from downloader import download_past_data
import time

import os

from flask import Flask
from flask_restful import Resource, Api


import dash_mantine_components as dmc
from dash import (
    html, callback, Output, Input, clientside_callback, Dash, no_update, State, dcc, ALL, ctx,
    _dash_renderer 
)
from dash_iconify import DashIconify
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from backtester import calculate_ratios
from historical_binance import BinanceDataProvider
from backtester import Backtester


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)







FOLDER_PATH = os.environ.get("DATA_FOLDER", os.path.realpath(os.path.join(os.path.dirname(__file__), "my_data/data/")))
CACHE_FILE = os.environ.get("CACHE_FILE", "caches.csv")
LIMIT_COPYTRADED = os.environ.get("LIMIT_COPYTRADED", "false").lower() == "true"

logger.info(f"Data folder: {FOLDER_PATH}")
logger.info(f"Cache file: {CACHE_FILE}")
logger.info(f"Limit to copytraded: {LIMIT_COPYTRADED}")


try:
    
    
    
    
    logger.warning("USING PLACEHOLDER BACKTESTER - REPLACE WITH ACTUAL IMPLEMENTATION")
    backtest_engine = Backtester(main_path=FOLDER_PATH, limit_to_copytraded=LIMIT_COPYTRADED, cache_file_name=CACHE_FILE)
    
except ImportError:
    logger.exception("Failed to import Backtester. Please ensure it's installed and the path is correct.")
    sys.exit(1)
except Exception as e:
    logger.exception(f"Failed to initialize Backtester: {e}")
    sys.exit(1)



_dash_renderer._set_react_version("18.2.0")

server = Flask(__name__)

dash_app = Dash(
    __name__,
    server=server,
    prevent_initial_callbacks=True, 
    external_stylesheets=[dmc.styles.ALL] 
)
api = Api(server)
def convert_tf(timeframe: str) -> int:
    """
    Converts a timeframe string to minutes.
    :param timeframe: Timeframe string (e.g., '1m', '5m', '1h', '1d').
    :return: Timeframe in minutes.
    """
    if timeframe.endswith("m"):
        return int(timeframe[:-1])
    elif timeframe.endswith("h"):
        return int(timeframe[:-1]) * 60
    elif timeframe.endswith("d"):
        return int(timeframe[:-1]) * 1440
    else:
        raise ValueError(f"Unsupported timeframe format: {timeframe}")

class HelloWorld(Resource):
    def get(self):
        logger.info("Hello World API endpoint was requested")
        return {'hello': 'world'}

api.add_resource(HelloWorld, '/hello')


def image_to_base64(image_path):
    """Converts an image file to a base64 string for embedding."""
    if not image_path or not os.path.exists(image_path):
        logger.debug(f"Image file not found or path empty: {image_path}")
        return "" 
    try:
        with open(image_path, "rb") as image_file:
            encoded_string = base64.b64encode(image_file.read()).decode()
        
        return f"data:image/jpeg;base64,{encoded_string}"
    except Exception as e:
        logger.exception(f"Could not render image at {image_path} due to {e}")
        return "" 

def generate_anchor(platform_type: str, trader_address: str):
    """Generates the external link URL for a trader based on platform."""
    base_urls = {
        "okx": f"https://www.okx.com/copy-trading/account/{trader_address}?tab=swap",
        "bybit": f"https://www.bybit.com/copyTrade/trade-center/detail?leaderMark={trader_address}&profileDay=90",
        "binance": f"https://www.binance.com/en/copy-trading/lead-details/{trader_address}",
        "feather": "", 
        "hl": "",
        "okxacc": "",
        "discord": ""
    }
    url = base_urls.get(str(platform_type).lower(), "")
    if not url:
        logger.debug(f"No URL template found for platform type: {platform_type}")
    return url


def generate_backtest_cards(sorting_method='rating'):
    """Generates a list of DMC Cards for traders based on cached data."""
    logger.info(f"Generating trader cards sorted by: {sorting_method}")
    backtest_engine.backtest_cacher._load_cache()
    all_backtest_cards = []
    try:
        
        if sorting_method == "good_traders":
            
            
            cache = backtest_engine.backtest_cacher.retrieve_good_traders() 
            if cache is None or cache.is_empty():
                logger.warning("Good trader cache is empty or None.")
                return [dmc.Alert("No 'Good Traders' found.", color="orange", title="Info")]
        else:
            cache = backtest_engine.backtest_cacher.obtain_cache_extended(sorting_method)[:300]
            if cache is None or cache.is_empty():
                logger.warning(f"Trader cache is empty or None for sorting: {sorting_method}")
                return [dmc.Alert(f"No traders found for sorting '{sorting_method}'.", color="orange", title="Info")]

        
        good_trader_addresses = set()
        
        
        
        try:
            good_traders_df = backtest_engine.backtest_cacher.retrieve_good_traders()
            if good_traders_df is not None and not good_traders_df.is_empty():
                good_trader_addresses = set(good_traders_df["Trader Address"].to_list())
            logger.info(f"Found {len(good_trader_addresses)} 'good' traders for status check.")
        except Exception as e_good:
            logger.warning(f"Could not retrieve good traders list for status badge: {e_good}")

    except Exception as e:
        logger.exception(f"Failed to retrieve or process trader cache: {e}")
        return [dmc.Alert("Error loading trader data. Check backend logs.", color="red", title="Cache Error")]

    
    max_values = {}
    all_modes = set()
    if "modes" in cache.columns:
        
        modes_list = cache["modes"].drop_nulls().to_list()
        for modes_str in modes_list:
            try:
                parsed_modes = json.loads(modes_str)
                if isinstance(parsed_modes, list):
                    all_modes.update(parsed_modes)
                elif isinstance(parsed_modes, str): 
                    all_modes.add(parsed_modes)
            except (json.JSONDecodeError, TypeError):
                logger.debug(f"Could not parse modes string: {modes_str}")
    else:
        logger.warning("Column 'modes' not found in cache.")


    logger.info(f"Found modes across traders: {all_modes}")
    for mode in all_modes:
        max_values[mode] = {}
        for metric in ["sharpe_ratio", "calmar_ratio", "days_traded", "information_ratio", "downside_capture_ratio", "calmar_divergence"]:
            col_name = f"{metric}_{mode}"
            if col_name in cache.columns:
                
                max_val = cache[col_name].max()
                max_values[mode][metric] = float(max_val) if max_val is not None and max_val != 0 else 1.0
            else:
                max_values[mode][metric] = 1.0 

    
    
    cache = cache.fill_null(0.0) 
    

    
    for trader in cache.iter_rows(named=True):
        try:
            trader_address = trader["Trader Address"]
            tagname = trader.get("tagname", "Unknown Trader")
            platform_type = trader.get("type", "")
            
            is_blacklisted = not trader.get("available", True)
            
            is_good_trader = trader_address in good_trader_addresses

            
            common_info = [
                
                dmc.Card(
                    dmc.Image(
                        src=image_to_base64(trader.get("file_thumb_path", "")),
                        
                        alt=f"{tagname} thumbnail",
                        
                        
                    ),
                    withBorder=True,
                    p=0 
                ),
                
                dmc.Group(
                    [
                        dmc.Text(
                            tagname,
                            fw=500, 
                            
                            c=("red" if is_blacklisted else "dimmed" if not is_good_trader else "black"),
                            
                            fs=("italic" if is_blacklisted else "normal"),
                            td=("line-through" if is_blacklisted else "none") 
                        ),
                        
                        dmc.Anchor(
                            DashIconify(icon="feather:external-link", width=16),
                            href=generate_anchor(platform_type, trader_address),
                            target="_blank",
                            ml="auto", 
                        ) if generate_anchor(platform_type, trader_address) else None, 
                        
                        dmc.Badge(
                            "Good" if is_good_trader else ("Blacklisted" if is_blacklisted else "Regular"),
                            color="green" if is_good_trader else ("red" if is_blacklisted else "gray"),
                            variant="light",
                            ml=4 
                        ),
                    ],
                    justify="space-between", 
                    mt="sm", 
                    preventGrowOverflow=False 
                ),
                dmc.Divider(my="xs")
            ]

            
            metrics_groups = []

            
            try:
                
                updated_ts = trader.get('date_updated', 0)
                date_updated = datetime.fromtimestamp(updated_ts).strftime("%d/%m/%y %H:%M") if updated_ts else "N/A"
            except Exception:
                date_updated = "N/A"

            default_badge_data = [
                ("🗿", trader.get("performance", 0.0), {"from": "indigo", "to": "cyan"}),
                ("⚖️", trader.get("trade_type_proportion_ratio", 0.0), {"from": "teal", "to": "lime"}),
                ("👌", trader.get("officialR", 0.0), {"from": "teal", "to": "lime"}),
                ("🏆", trader.get("rating", 0), {"from": "grape", "to": "pink"}), 
                ("📈", f'{trader.get("PNL", 0.0):,.0f}', "blue"), 
                ("📈m", f'{trader.get("PNLMonth", 0.0):,.0f}', "blue"),
                ("$", f'{trader.get("ACCValue", 0.0):,.0f}', "blue"),
                ("⌛", date_updated, "gray"),
                ("🪪", f"{trader.get('Trader Address')}", "yellow")
            ]

            default_badges = []
            for label, value, gradient_or_color in default_badge_data:
                badge_props = {
                    "children": label + " " + str(value),
                    
                    
                    
                    
                    
                    
                    "variant": "gradient" if isinstance(gradient_or_color, dict) else "light", 
                    
                }
                if isinstance(gradient_or_color, dict):
                    badge_props["gradient"] = gradient_or_color
                else:
                    badge_props["color"] = gradient_or_color 

                default_badges.append(dmc.Badge(**badge_props))

            
            metrics_groups.append(dmc.SimpleGrid(
                cols=2, 
                
                verticalSpacing="xs",
                children=default_badges,
                mt="xs"
            ))


            
            current_modes = []
            modes_json = trader.get("modes")
            if modes_json and isinstance(modes_json, str): 
                try:
                    loaded_modes = json.loads(modes_json)
                    if isinstance(loaded_modes, list):
                        current_modes = loaded_modes
                    elif isinstance(loaded_modes, str): 
                        current_modes = [loaded_modes]
                except (json.JSONDecodeError, TypeError):
                    logger.debug(f"Could not decode modes for trader {trader_address}: {modes_json}")


            if current_modes: 
                mode_metrics_elements = []
                for mode in current_modes:
                    if mode not in max_values: 
                        logger.debug(f"Skipping mode '{mode}' for {tagname}, max values not found.")
                        continue

                    sharpe = trader.get(f"sharpe_ratio_{mode}", 0.0)
                    calmar = trader.get(f"calmar_ratio_{mode}", 0.0)
                    days_traded = trader.get(f"days_traded_{mode}", 0.0)
                    information = trader.get(f"information_ratio_{mode}", 0.0)
                    
                    downside_capture = trader.get(f"downside_capture_ratio_{mode}", 0.0) 
                    calmar_divergence = trader.get(f"calmar_divergence_{mode}", 0.0)
                    gittins_index = trader.get(f"gittins_index_{mode}", 0.0)

                    
                    rel_sharpe = (sharpe / max_values[mode]["sharpe_ratio"]) if max_values[mode].get("sharpe_ratio") and max_values[mode]["sharpe_ratio"] != 0 else 0.0
                    rel_calmar = (calmar / max_values[mode]["calmar_ratio"]) if max_values[mode].get("calmar_ratio") and max_values[mode]["calmar_ratio"] != 0 else 0.0
                    rel_days = (days_traded / max_values[mode]["days_traded"]) if max_values[mode].get("days_traded") and max_values[mode]["days_traded"] != 0 else 0.0
                    rel_info = (information / max_values[mode]["information_ratio"]) if max_values[mode].get("information_ratio") and max_values[mode]["information_ratio"] != 0 else 0.0

                    
                    
                    
                    rel_dcr = (downside_capture / max_values[mode]["downside_capture_ratio"]) if max_values[mode].get("downside_capture_ratio") and max_values[mode]["downside_capture_ratio"] != 0 else 0.0

                    
                    rel_cd = (calmar_divergence / max_values[mode]["calmar_divergence"]) if max_values[mode].get("calmar_divergence") and max_values[mode]["calmar_divergence"] != 0 else 0.0
                    rel_gi = (gittins_index / max_values[mode]["gittins_index"]) if max_values[mode].get("gittins_index") and max_values[mode]["gittins_index"] != 0 else 0.0


                    
                    sr_clamped = max(0.0, min(1.0, rel_sharpe))
                    cr_clamped = max(0.0, min(1.0, rel_calmar))
                    dt_clamped = max(0.0, min(1.0, rel_days))
                    ir_clamped = max(0.0, min(1.0, rel_info))

                    
                    
                    
                    dcr_clamped_for_color_input = max(0.0, min(1.0, 1.0 - rel_dcr)) 

                    cd_clamped = max(0.0, min(1.0, rel_cd)) 
                    gi_clamped = max(0.0, min(1.0, rel_gi))

                    
                    def get_color(value_clamped):
                        red = int(255 * (1 - value_clamped))
                        green = int(255 * value_clamped)
                        return f"rgb({red}, {green}, 0)"

                    sharpe_color = get_color(sr_clamped)
                    calmar_color = get_color(cr_clamped)
                    days_traded_color = get_color(dt_clamped)
                    ir_color = get_color(ir_clamped)
                    
                    dcr_color = get_color(dcr_clamped_for_color_input)
                    cd_color = get_color(cd_clamped)
                    gi_color = get_color(gi_clamped)

                    

                    mode_badge_data = [
                        ("S", round(sharpe, 2), sharpe_color),
                        ("C", round(calmar, 2), calmar_color),
                        ("D", int(days_traded), days_traded_color), 
                        ("IR", round(information, 2), ir_color),
                        
                        ("DCR", f"{round(downside_capture, 1)}%", dcr_color), 
                        ("CD", round(calmar_divergence, 2), cd_color),
                        ("G", round(float(gittins_index), 2) if gittins_index is not None else 0, gi_color),
                    ]

                    badges_current_mode = []
                    for label, value, bg_color in mode_badge_data:
                        badges_current_mode.append(
                            dmc.Tooltip( 
                                label=f"{'Sharpe' if label=='S' else 'Calmar' if label=='C' else 'Days Traded' if label == 'D' else 'Information Ratio' if label == 'IR' else 'Downside Capture Ratio' if label == 'DCR' else 'Calmar Divergence' if label == 'CD' else 'Gittins Index'}: {value}",
                                children=[
                                    dmc.Badge(
                                        
                                        leftSection=dmc.Badge(label, variant="outline", color="gray", size="xs"),
                                        children=value,
                                        
                                        variant="filled", 
                                        color="transparent", 
                                        style={"backgroundColor": bg_color}, 
                                        size="sm"
                                    )
                                ],
                                withArrow=True
                            )
                        )

                    mode_metrics_elements.extend([
                        dmc.Divider(label=mode, labelPosition="center", variant='dotted', my=2),
                        dmc.Group(badges_current_mode, justify="space-around", gap="xs") 
                    ])
                if mode_metrics_elements: 
                    metrics_groups.append(dmc.Stack(mode_metrics_elements, gap=2, mt="xs")) 

            
            
            rating = trader.get("rating", 0)
            max_acc_value = trader.get("MaxACCValue", 0.0)
            available = trader.get("available", True)
            performance = trader.get("performance", 0.0)

            
            days_traded_normal = trader.get("days_traded_normal", 0.0)
            calmar_ratio_normal = trader.get("calmar_ratio_normal", 0.0)
            sharpe_ratio_normal = trader.get("sharpe_ratio_normal", 0.0)
            csv_line = (
                f'"{trader_address}",'
                f'{rating},'
                f'{max_acc_value},'
                f'"{available}",'
                f'{performance},'
                f'{days_traded_normal},'
                f'{calmar_ratio_normal},'
                f'{sharpe_ratio_normal}'
            )
            action_group = [
                dmc.Divider(my="sm"),
                
                dmc.Accordion(
                    chevronPosition="left",
                    variant="contained",
                    radius="sm",
                    children=[
                        dmc.AccordionItem(
                            [
                                dmc.AccordionControl(
                                    "View Setup JSON",
                                    icon=DashIconify(icon="carbon:code", width=16)
                                ),
                                dmc.AccordionPanel(
                                    dmc.Code(
                                        
                                        children=csv_line,
                                        
                                        
                                    )
                                ),
                            ],
                            value="setup-json", 
                        )
                    ],
                ),
                
                dmc.SimpleGrid(
                    cols=1, 
                    
                    mt="sm",
                    children=[
                        
                        dmc.Button(
                            "Backtest Now",
                            variant="light", 
                            id={"type": "button-backtest", "index": trader_address},
                            color="blue",
                            fullWidth=True,
                            leftSection=DashIconify(icon="carbon:play"), 
                            size="xs"
                        ),
                        dmc.Button(
                            "View Past Render",
                            variant="outline",
                            id={"type": "button-view-render", "index": trader_address},
                            color="green",
                            fullWidth=True,
                            leftSection=DashIconify(icon="carbon:chart-line-data"),
                            size="xs"
                        ),
                        dmc.Button(
                            "HyperOpt Leverage",
                            variant="outline",
                            id={"type": "button-hyperopt-leverage", "index": trader_address},
                            color="orange",
                            fullWidth=True,
                            leftSection=DashIconify(icon="carbon:analytics"),
                            size="xs"
                        ),
                    ]
                ),
            ]

            
            all_backtest_cards.append(dmc.Card(
                children=common_info + metrics_groups + action_group,
                withBorder=True,
                shadow="sm",
                radius="md",
                
                p="sm" 
            ))
        except Exception as e:
            trader_id_err = trader.get('Trader Address', 'UNKNOWN')
            logger.error(f"Failed to generate card for trader {trader_id_err}: {e} - Traceback: {traceback.format_exc()}")
            
            all_backtest_cards.append(dmc.Card(
                children=[dmc.Alert(f"Error loading card for {trader.get('tagnmame', 'error')}", color="red", title="Card Error", icon=DashIconify(icon="carbon:error"))],
                withBorder=True, shadow="sm", radius="md", p="sm" 
            ))

    logger.info(f"Generated {len(all_backtest_cards)} trader cards.")
    
    return dmc.SimpleGrid(
        cols={"base": 1, "sm": 2, "lg": 3, "xl": 4}, 
        
        children=all_backtest_cards,
        p="md" 
    )


def create_btc_metrics_card():
    """Calculates and creates a card for BTC metrics."""
    try:
        btc_ticker = "BTC/USDT:USDT"
        timeframe_str = "1m"
        days = 365 * 5

        download_past_data([btc_ticker], "bybit", convert_tf(timeframe_str), day_count=days)
        
        ticker_path = os.path.join(FOLDER_PATH, "bybit", "futures")
        ticker_name_template = "{currency}_USDT_USDT-{timeframe}-futures.feather"
        
        provider = BinanceDataProvider([btc_ticker], [timeframe_str], ticker_path, ticker_name_template)
        asyncio.run(provider.load_tickers())
        
        btc_df = provider.cached_dataframes[timeframe_str][btc_ticker]
        
        if btc_df is None or btc_df.is_empty():
            return dmc.Card(children=[dmc.Text("BTC data not found.")], withBorder=True, shadow="sm", radius="md", p="sm")
            
        btc_price_series = btc_df['close']
        ratios = calculate_ratios(btc_price_series, timeframe_in_minutes=convert_tf(timeframe_str))
        
        sharpe = ratios.get('sharpe_ratio', 0.0)
        gittins = ratios.get('gittins_index', 0.0)
        
        card = dmc.Card(
            children=[
                dmc.Text("BTC Market State (5Y, 1D)", fw=500),
                dmc.Space(h="sm"),
                dmc.Group([dmc.Text("Sharpe:"), dmc.Text(f"{sharpe:.2f}")]),
                dmc.Group([dmc.Text("Gittins:"), dmc.Text(f"{gittins:.2f}")]),
            ],
            withBorder=True, shadow="sm", radius="md", p="sm"
        )
        return card

    except Exception as e:
        logger.error(f"Failed to create BTC metrics card: {e}", exc_info=True)
        return dmc.Card(children=[dmc.Text("Error loading BTC metrics.")], withBorder=True, shadow="sm", radius="md", p="sm")



entry_cards_controls = [
    create_btc_metrics_card(),
    
    dmc.Card(
        children=[
            dmc.Select(
                label="Sort Displayed Traders",
                placeholder="Select sorting",
                id="sorting",
                style={"z-index": 1000},

                value="rating", 
                data=[ 
                    {"value": "rating", "label": "ELO Rating"},
                    {"value": "performance", "label": "Performance"},
                    {"value": "officialR", "label": "Official Rating"},
                    {"value": "good_traders", "label": "Good Traders Only"} 
                ],
                
                
            )
        ],
        withBorder=True, shadow="sm", radius="md", p="sm"
    ),
    
    dmc.Card(
        children=[
            dmc.Text("Add Trader Manually", fw=500, mb="sm"),
            dmc.Stack([
                dmc.TextInput(label="Trader Name:", id="trader-name", placeholder="e.g., Cool Trader"),
                dmc.TextInput(label="Trader Code/Address:", id="trader-tag", placeholder="Enter unique ID or address"),
                dmc.SegmentedControl(
                    id="trader-platform",
                    value="okx", 
                    data=[ 
                        {"value": "feather", "label": "FEATHER"},
                        {"value": "hl", "label": "HYPE"},
                        {"value": "okxacc", "label": "OKX Acc"},
                        {"value": "okx", "label": "OKX"},
                        {"value": "bybit", "label": "Bybit"},
                        {"value": "discord", "label": "Discord"},
                        {"value": "binance", "label": "Binance"}
                    ],
                    mt="sm",
                    fullWidth=True,
                    color="blue"
                ),
            ]),
            dmc.Button(
                "Add and Backtest",
                variant="light",
                color="blue",
                fullWidth=True,
                mt="md",
                radius="md",
                
                id={"type": "button-backtest", "index": "add-new-trader"},
                leftSection=DashIconify(icon="carbon:add-alt"),
            ),
        ],
        withBorder=True, shadow="sm", radius="md", p="sm"
    ),
    
    dmc.Card(
        children=[
            dmc.Text("Add/Run Trader via JSON", fw=500, mb="sm"),
            dmc.Textarea(
                label="Trader JSON:",
                id="json-trader-input",
                placeholder='[{"Trader Address": "...", "type": "...", "tagname": "..."}]',
                autosize=True,
                minRows=4,
                maxRows=10,
            ),
            dmc.Button(
                "Backtest JSON",
                variant="light",
                color="green",
                fullWidth=True,
                mt="md",
                radius="md",
                id="bt-bt-json", 
                leftSection=DashIconify(icon="carbon:code"),
            ),
        ],
        withBorder=True, shadow="sm", radius="md", p="sm"
    ),
]



drawer = dmc.Drawer(
    title="Trader Management & Backtest",
    id="drawer-simple",
    padding="md",
    size="85%", 
    
    position="left", 
        children=[
            dmc.Stack( 
                children=[
                    
                    dmc.SimpleGrid(
                        cols={"base": 1, "md": 3}, 
                        
                        children=entry_cards_controls,
                        mb="md" 
                    ),
                    dmc.Divider(label="Available Traders", labelPosition="center"),
                    
                    
                    dmc.ScrollArea(
                        
                        children=html.Div(dmc.Center(dmc.Loader(variant="bars"), style={"height": 200}), id="drawer-cards-container"),
                        h="calc(100vh - 350px)", 
                        type="auto" 
                    )
                ],
                gap="sm" 
            )
        ]

)


dash_app.layout = dmc.MantineProvider( 
    theme={"colorScheme": "light"}, 
    
    
    children=[
        
        

        
        
        
                dmc.Stack( 
                    gap="lg", 
                    children=[
                        
                        dmc.Paper(
                            p="md", shadow="xs", withBorder=True, radius="md",
                            children=[
                                dmc.Title("Combined Backtest Configuration", order=3, mb="md"),
                                dmc.Grid(
                                    gutter="lg", 
                                    children=[
                                        
                                        dmc.GridCol(
                                            dmc.MultiSelect(
                                                label="Select Traders for Combined Backtest",
                                                placeholder="Select one or more 'Good' traders from list",
                                                id="trader-multi-select",
                                                value=[],
                                                data=[], 
                                                searchable=True,
                                                clearable=True,
                                                
                                                
                                            ),
                                            span={"xs": 12, "md": 6, "lg": 4} 
                                        ),

                                        
                                        dmc.GridCol(
                                            dmc.Group( 
                                                
                                                align="flex-end", 
                                                
                                                children=[
                                                    dmc.Select(
                                                        label="Leverage type",
                                                        id="leverage-type", value="static",
                                                        data=[
                                                            {"value": "atr", "label": "Dynamic ATR"},
                                                            {"value": "static", "label": "Static"},
                                                            {"value": "diff", "label": "Differential"},
                                                        ],
                                                        style={"flex": 1} 
                                                    ),
                                                    dmc.NumberInput(
                                                        label="Leverage parameter", id="leverage-parameter",
                                                        value=2, min=1, step=0.5,  max=10000, 
                                                        style={"flex": 1}
                                                    ),
                                                    dmc.NumberInput(
                                                        label="ATR length", id="atr-length",
                                                        value=385, min=50, step=1, max=50000,
                                                        disabled=True, 
                                                        style={"flex": 1}
                                                    ),
                                                ]
                                            ),
                                            span={"xs": 12, "md": 6, "lg": 5}
                                        ),

                                        
                                        dmc.GridCol(
                                            dmc.Group(
                                                align="flex-end",
                                                
                                                children=[
                                                    dmc.NumberInput(
                                                        label="Yolomax quantile", id="yolomax-parameter",
                                                        value=.1, max=1, min=0.01, step=0.01,
                                                        style={"flex": 1}
                                                    ),
                                                    dmc.NumberInput(
                                                        label="Yolometer quantile", id="yolometer-parameter",
                                                        value=.1, max=1, min=0.01, step=0.01,
                                                        style={"flex": 1}
                                                    ),
                                                    dmc.Checkbox(
                                                        id="margin-scale", label="Marginal scaler", checked=False,
                                                        
                                                        style={"paddingBottom": "4px"} 
                                                    ),
                                                ]
                                            ),
                                            span={"xs": 12, "lg": 3}
                                        ),

                                        
                                        dmc.GridCol(
                                            dmc.Flex( 
                                                dmc.Button(
                                                    "Run Combined Backtest", id="run-iteration",
                                                    leftSection=DashIconify(icon="carbon:play-filled")
                                                ),
                                                align="flex-end", 
                                                justify="center", 
                                                h="100%" 
                                            ),
                                            span={"xs": 12, "lg": "auto"} 
                                        ),
                                    ]
                                )
                            ]
                        ),

                        
                        dmc.Paper( 
                            shadow="xs", withBorder=True, radius="md", p="xs", 
                            children=[
                                html.Div( 
                                    children=dcc.Graph(
                                        id="graph-output",
                                        
                                        style={"height": "65vh"}, 
                                        figure=go.Figure().update_layout( 
                                            title="Graph Output Area",
                                            xaxis={"visible": False}, yaxis={"visible": False},
                                            annotations=[{"text": "Run a backtest or view render to see graph", "xref": "paper", "yref": "paper", "showarrow": False, "font": {"size": 16}}]
                                        )
                                    ),
                                    id="graph-container"
                                )
                            ]
                        )
                
                
            ],
            
            
            
        ),

        
        dmc.Affix(
            dmc.Button(
                "Manage Traders",
                id="drawer-demo-button",
                leftSection=DashIconify(icon="carbon:menu"), 
                variant="gradient", 
                gradient={"from": "indigo", "to": "cyan"}
            ),
            position={"bottom": 20, "right": 20} 
        ),

        
        drawer,

        
        dcc.Store(id='current-iteration-setup', storage_type='memory'), 
        dcc.Store(id='latest-graph-figure', storage_type='memory') 
    ]
)






@callback(
    Output("drawer-cards-container", "children"),
    
    Input("sorting", "value"),
    
    
    
)
def sorting_method_update(sort_value):
    logger.info(f"Sorting method changed to: {sort_value}. Regenerating cards.")
    
    try:
        cards = generate_backtest_cards(sort_value)
        return cards, False 
    except Exception as e:
        logger.exception(f"Error generating cards for sort '{sort_value}': {e}")
        error_message = dmc.Alert(
            f"Failed to load cards for sorting '{sort_value}'. Error: {e}",
            color="red", title="Card Loading Error", withCloseButton=True
        )
        return error_message 



clientside_callback(
    """
    function(n_clicks) {
        // Basic check to ensure the callback is triggered by a click
        if (n_clicks === undefined || n_clicks === null || n_clicks === 0) {
            // Use dash_clientside namespace for no_update
            return dash_clientside.no_update;
        }
        // Return true to open the drawer
        return true;
    }
    """,
    Output('drawer-simple', 'opened', allow_duplicate=True), 
    Input("drawer-demo-button", 'n_clicks'),
    prevent_initial_call=True 
)




clientside_callback(
    """
    function(leverage_type_value) {
        // Default values
        let min = 1, max = 10000, step = 0.5, value = 2, precision = 2, atrDisabled = true;

        if (leverage_type_value === "atr") {
            min = 0.001; max = 0.15; step = 0.001; value = 0.03; precision = 3; atrDisabled = false;
        } else if (leverage_type_value === "static") {
            min = 1; max = 50; step = 0.5; value = 2; precision = 2; atrDisabled = true;
        } else if (leverage_type_value === "diff") {
            // Assuming 'diff' uses a 3-digit integer representation
            min = 111; max = 999; step = 1; value = 321; precision = 0; atrDisabled = true;
        }
        // Return values in the order of Outputs
        // Ensure allow_duplicate=True is set on these Output properties in the Python callback definition
        return [min, max, step, value, atrDisabled];
    }
    """,
    Output('leverage-parameter', 'min', allow_duplicate=True),
    Output('leverage-parameter', 'max', allow_duplicate=True),
    Output('leverage-parameter', 'step', allow_duplicate=True),
    Output('leverage-parameter', 'value', allow_duplicate=True),
    Output('atr-length', 'disabled', allow_duplicate=True),
    Input("leverage-type", 'value'),
    prevent_initial_call=True 
)






@callback(
    Output("trader-multi-select", "data"),
    Output("trader-multi-select", "value"), 
    
    Input("latest-graph-figure", "data"), 
    
    
    State("trader-multi-select", "value"), 
    prevent_initial_call=True 
)
def update_multiselect_options(graph_data_updated, current_selection):
    
    triggered_input = ctx.triggered_id
    logger.info(f"Updating multi-select trader options triggered by: {triggered_input}")
    try:
        goodtr = backtest_engine.backtest_cacher.retrieve_good_traders()
        if goodtr is None or goodtr.is_empty():
            logger.warning("No good traders found to populate multi-select.")
            return [], [] 

        
        data_select_options = [
            {"value": trader["Trader Address"], "label": trader["tagname"]}
            for trader in goodtr.iter_rows(named=True)
        ]

        
        valid_trader_addresses = {option["value"] for option in data_select_options}
        filtered_selection = [val for val in current_selection if val in valid_trader_addresses]

        logger.info(f"Multi-select options updated. {len(data_select_options)} good traders available.")
        return data_select_options, filtered_selection

    except Exception as e:
        logger.exception("Failed to update multi-select options")
        
        return [], [] 





@callback(
    Output("graph-output", "figure", allow_duplicate=True),
    Output("latest-graph-figure", "data", allow_duplicate=True), 
    Output("current-iteration-setup", "data", allow_duplicate=True), 
    
    Input("run-iteration", 'n_clicks'),
    State("leverage-type", "value"),
    State("leverage-parameter", "value"),
    State("atr-length", "value"),
    State("trader-multi-select", "value"), 
    State("yolomax-parameter", "value"),
    State("yolometer-parameter", "value"),
    State("margin-scale", "checked"),
    prevent_initial_call=True
)
def run_combined_backtest(n_clicks, leverage_type, leverage_parameter, atr_length, selected_traders, yolomax_param, yolometer_param, margin_scale):
    start_time = time.time()
    if not n_clicks or not selected_traders:
        logger.info("Combined backtest not run: No clicks or no traders selected.")
        return no_update, no_update, no_update 

    logger.info(f"Initiating combined backtest for {len(selected_traders)} traders: {selected_traders}")

    try:
        
        all_traders_cache = backtest_engine.backtest_cacher.obtain_cache_extended('rating') 
        if all_traders_cache is None or all_traders_cache.is_empty():
            raise ValueError("Failed to retrieve trader cache.")

        
        
        setups_df = all_traders_cache.filter(pl.col("Trader Address").is_in(selected_traders)).select(["setup", "tagname"])

        current_iteration_setups = []
        setup_tagnames = []
        if not setups_df.is_empty():
            for row in setups_df.iter_rows(named=True):
                try:
                    setup_obj = json.loads(row["setup"])
                    
                    if isinstance(setup_obj, dict):
                        current_iteration_setups.append(setup_obj)
                    elif isinstance(setup_obj, list):
                        current_iteration_setups.extend(setup_obj) 
                    setup_tagnames.append(row["tagname"])
                except json.JSONDecodeError:
                    logger.warning(f"Could not parse setup JSON for trader {row.get('tagname', 'Unknown')}")
        else:
            logger.warning("No setup data found for selected traders in the cache.")
            fig = go.Figure().update_layout(title_text="Error: No setup found for selected traders")
            return fig, fig.to_plotly_json(), None 


        if not current_iteration_setups:
            logger.warning("No valid setups found for selected traders after parsing.")
            fig = go.Figure().update_layout(title_text="Error: No valid setups found for selected traders")
            return fig, fig.to_plotly_json(), None

        
        leverage_setup = {}
        lev_str = f"type={leverage_type}"
        if leverage_type == "diff":
            
            try:
                param_int = int(leverage_parameter)
                high = param_int // 100
                mid = (param_int % 100) // 10
                low = param_int % 10
                leverage_setup = {"leverage": {"BTC/USDT:USDT": high, "ETH/USDT:USDT": mid, "OTHER": low}, "type": leverage_type}
                lev_str += f" H={high}, M={mid}, L={low}"
            except (ValueError, TypeError):
                logger.error(f"Invalid leverage parameter for diff type: {leverage_parameter}")
                raise ValueError("Invalid 'diff' leverage parameter. Must be 3 digits.")
        else:
            leverage_setup = {"leverage": leverage_parameter, "type": leverage_type, "atr_length": atr_length, "atr_minutes": 5}
            lev_str += f" param={leverage_parameter}"
            if leverage_type == "atr":
                lev_str += f" len={atr_length}"


        quantile_setup = {"yolomax": yolomax_param, "yolometer": yolometer_param}
        quantile_str = f"yMax={yolomax_param}, yMet={yolometer_param}"
        margin_str = f"marginScale={margin_scale}"

        logger.info(f"Running combined backtest ({len(current_iteration_setups)} setups) with Lev: {lev_str}, Quant: {quantile_str}, {margin_str}")

        backtest_engine.reset_downloaded_queue() 
        
        
        
        
        response = asyncio.run(backtest_engine.backtest_traders(current_iteration_setups, leverage_setup=leverage_setup, quantile_setup=quantile_setup, margin_scale=margin_scale))
        elapsed = time.time() - start_time
        logger.info(f"Combined backtest completed in {elapsed:.2f} seconds.")


        if response and "graph" in response and isinstance(response["graph"], go.Figure):
            figure = response["graph"]
            
            title = f"Combined: {', '.join(setup_tagnames[:3])}{'...' if len(setup_tagnames)>3 else ''} | Lev: {lev_str} | {quantile_str} | {margin_str}"
            figure.update_layout(
                title=title,
                
                uirevision=f"combined-{n_clicks}-{leverage_type}-{leverage_parameter}"
            )
            
            return figure, figure.to_plotly_json(), current_iteration_setups
        else:
            logger.error("Combined backtest failed or returned no valid graph.")
            error_fig = go.Figure().update_layout(title_text="Error during combined backtest or no graph returned")
            return error_fig, error_fig.to_plotly_json(), current_iteration_setups 

    except Exception as e:
        elapsed = time.time() - start_time
        logger.exception(f"Error during combined backtest execution ({elapsed:.2f}s): {e}")
        error_fig = go.Figure().update_layout(title_text=f"Combined Backtest Error: {e}")
        
        failed_setup = locals().get("current_iteration_setups", None)
        return error_fig, error_fig.to_plotly_json(), failed_setup



@callback(
    Output("graph-output", "figure", allow_duplicate=True),
    Output("latest-graph-figure", "data", allow_duplicate=True),
    Output("current-iteration-setup", "data", allow_duplicate=True),
    
    
    Input({"type": "button-view-render", "index": ALL}, 'n_clicks'),
    prevent_initial_call=True,
    running=[(Output("drawer-simple", "opened", allow_duplicate=True), False, True)] 
)
def view_past_render(n_clicks):
    start_time = time.time()
    triggered_id_dict = ctx.triggered_id
    
    if not triggered_id_dict or not isinstance(triggered_id_dict, dict) or not any(n is not None and n > 0 for n in n_clicks):
        logger.debug("View past render not triggered by button click.")
        return no_update, no_update, no_update

    trader_address = triggered_id_dict["index"]
    logger.info(f"Fetching past render for trader address: {trader_address}")

    try:
        
        trader_info_df = backtest_engine.backtest_cacher.obtain_cache_extended('rating').filter(pl.col("Trader Address") == trader_address)

        if trader_info_df.is_empty():
            logger.error(f"Could not find trader info for address: {trader_address}")
            fig = go.Figure().update_layout(title_text=f"Error: Trader {trader_address} not found in cache")
            return fig, fig.to_plotly_json(), None 

        
        trader_info = trader_info_df.row(0, named=True) 
        setup_str = trader_info.get("setup")
        tagname = trader_info.get("tagname", trader_address) 

        if not setup_str:
            logger.error(f"Setup JSON string is missing for trader: {trader_address}")
            fig = go.Figure().update_layout(title_text=f"Error: Setup missing for {tagname}")
            return fig, fig.to_plotly_json(), None

        try:
            
            setup_obj = json.loads(setup_str)
        except json.JSONDecodeError:
            logger.error(f"Invalid setup JSON for trader {tagname}: {setup_str}")
            fig = go.Figure().update_layout(title_text=f"Error: Invalid setup JSON for {tagname}")
            return fig, fig.to_plotly_json(), None 

        
        graph_fig = backtest_engine.backtest_cacher.fetch_graph(setup_obj)
        elapsed = time.time() - start_time

        if graph_fig and isinstance(graph_fig, go.Figure):
            logger.info(f"Past render fetched successfully for {tagname} in {elapsed:.2f}s.")
            graph_fig.update_layout(
                title=f"Past Render: {tagname}",
                
                uirevision=f"render-{trader_address}"
            )
            return graph_fig, graph_fig.to_plotly_json(), setup_obj 
        else:
            logger.warning(f"No cached graph found for trader {tagname} ({elapsed:.2f}s)")
            fig = go.Figure().update_layout(title_text=f"No cached graph found for {tagname}")
            
            return fig, fig.to_plotly_json(), setup_obj

    except Exception as e:
        elapsed = time.time() - start_time
        logger.exception(f"Error fetching past render for {trader_address} ({elapsed:.2f}s): {e}")
        fig = go.Figure().update_layout(title_text=f"Error fetching render: {e}")
        return fig, fig.to_plotly_json(), None 



@callback(
    Output("graph-output", "figure", allow_duplicate=True),
    Output("latest-graph-figure", "data", allow_duplicate=True),
    
    Output("current-iteration-setup", "data", allow_duplicate=True),
    Input({"type": "button-hyperopt-leverage", "index": ALL}, 'n_clicks'),
    prevent_initial_call=True,
    running=[(Output("drawer-simple", "opened", allow_duplicate=True), False, True)] 
)
def hyperopt_leverage(n_clicks):
    start_time = time.time()
    triggered_id_dict = ctx.triggered_id
    if not triggered_id_dict or not isinstance(triggered_id_dict, dict) or not any(n is not None and n > 0 for n in n_clicks):
        logger.debug("Hyperopt leverage not triggered by button click.")
        return no_update, no_update, no_update

    trader_address = triggered_id_dict["index"]
    logger.info(f"Starting Hyperopt Leverage for trader address: {trader_address}")

    base_setup_obj = None 
    try:
        
        trader_info_df = backtest_engine.backtest_cacher.obtain_cache_extended('rating').filter(pl.col("Trader Address") == trader_address)
        if trader_info_df.is_empty():
            raise ValueError(f"Trader info not found for Hyperopt: {trader_address}")

        trader_info = trader_info_df.row(0, named=True)
        setup_str = trader_info.get("setup")
        tagname = trader_info.get("tagname", trader_address)
        if not setup_str:
            raise ValueError(f"Setup JSON string missing for trader: {trader_address}")

        try:
            base_setup_obj = json.loads(setup_str) 
            
            trader_setup_to_run = [base_setup_obj] if isinstance(base_setup_obj, dict) else base_setup_obj
            if not isinstance(trader_setup_to_run, list):
                raise ValueError("Setup must be a dict or list of dicts")
        except json.JSONDecodeError:
            raise ValueError(f"Invalid setup JSON for trader {tagname}: {setup_str}")

        
        
        leverages_static = [{"leverage": round(0.5 * i + 1, 1), "type": "static", "atr_length": 1000, "atr_minutes": 5} for i in range(0, 19)] 
        atr_lengths = [100 + i * 500 for i in range(0, 4)] 
        atr_multipliers = [round(0.006 * ix, 3) for ix in range(1, 6)] 
        leverages_atr = [{"leverage": mult, "type": "atr", "atr_length": length, "atr_minutes": 5} for length in atr_lengths for mult in atr_multipliers]

        leverages_to_test = leverages_static + leverages_atr 
        logger.info(f"Testing {len(leverages_to_test)} leverage configurations for {tagname}...")

        
        results = {"static": {"leverage": [], "sharpe": [], "calmar": []}}
        
        results["atr"] = {length: [] for length in atr_lengths}

        
        
        
        for i, test_leverage_setup in enumerate(leverages_to_test):
            step_start_time = time.time()
            logger.debug(f"Hyperopt step {i+1}/{len(leverages_to_test)} for {tagname}: {test_leverage_setup}")

            
            response = asyncio.run(backtest_engine.backtest_traders(trader_setup_to_run, leverage_setup=test_leverage_setup))
            step_elapsed = time.time() - step_start_time

            
            if response and "ratios" in response and isinstance(response["ratios"], dict):
                ratios = response["ratios"]
                
                sharpe = ratios.get("sharpe_ratio_normal", 0.0)
                calmar = ratios.get("calmar_ratio_normal", 0.0)

                if test_leverage_setup["type"] == "static":
                    results["static"]["leverage"].append(test_leverage_setup["leverage"])
                    results["static"]["sharpe"].append(sharpe)
                    results["static"]["calmar"].append(calmar)
                elif test_leverage_setup["type"] == "atr":
                    atr_length = test_leverage_setup["atr_length"]
                    if atr_length in results["atr"]: 
                        results["atr"][atr_length].append({
                            "mult": test_leverage_setup["leverage"],
                            "sharpe": sharpe,
                            "calmar": calmar
                        })
                logger.debug(f"Step {i+1} ratios: Sharpe={sharpe:.2f}, Calmar={calmar:.2f} ({step_elapsed:.2f}s)")
            else:
                logger.warning(f"No valid ratio data returned for hyperopt step {i+1} ({step_elapsed:.2f}s)")
                
                

        
        logger.info("Generating Hyperopt results plot...")
        fig = make_subplots(
            rows=1, cols=2,
            subplot_titles=("Static Leverage Optimization", "ATR Leverage Optimization"),
            shared_yaxes=False 
        )

        
        if results["static"]["leverage"]:
            fig.add_trace(
                go.Scatter(
                    x=results["static"]["leverage"], y=results["static"]["sharpe"],
                    mode='lines+markers', name='Sharpe (Static)', legendgroup="static",
                    line=dict(color='blue'), marker=dict(symbol='circle')
                ), row=1, col=1
            )
            fig.add_trace(
                go.Scatter(
                    x=results["static"]["leverage"], y=results["static"]["calmar"],
                    mode='lines+markers', name='Calmar (Static)', legendgroup="static",
                    line=dict(color='red'), marker=dict(symbol='square')
                ), row=1, col=1
            )
        else:
            fig.add_annotation(text="No Static Data", xref="x domain", yref="y domain", x=0.5, y=0.5, showarrow=False, row=1, col=1)


        
        atr_plot_count = 0
        colors = ['green', 'orange', 'purple', 'brown'] 
        symbols = ['diamond', 'cross', 'triangle-up', 'star']
        for i, (length, data_list) in enumerate(results["atr"].items()):
            if data_list: 
                
                sorted_data = sorted(data_list, key=lambda k: k["mult"])
                multipliers = [d["mult"] for d in sorted_data]
                sharpes = [d["sharpe"] for d in sorted_data]
                calmars = [d["calmar"] for d in sorted_data]
                color = colors[i % len(colors)]
                symbol = symbols[i % len(symbols)]

                fig.add_trace(
                    go.Scatter(
                        x=multipliers, y=sharpes,
                        mode='lines+markers', name=f'Sharpe (ATR Len: {length})', legendgroup=f"atr_{length}",
                        line=dict(color=color, dash='solid'), marker=dict(symbol=symbol)
                    ), row=1, col=2
                )
                fig.add_trace(
                    go.Scatter(
                        x=multipliers, y=calmars,
                        mode='lines+markers', name=f'Calmar (ATR Len: {length})', legendgroup=f"atr_{length}",
                        line=dict(color=color, dash='dash'), marker=dict(symbol=symbol) 
                    ), row=1, col=2
                )
                atr_plot_count += 1

        if atr_plot_count == 0:
            fig.add_annotation(text="No ATR Data", xref="x2 domain", yref="y2 domain", x=0.5, y=0.5, showarrow=False, row=1, col=2)


        fig.update_layout(
            title_text=f'Leverage HyperOpt Results for {tagname}',
            xaxis_title="Static Leverage",
            xaxis2_title="ATR Multiplier",
            yaxis_title="Ratio Value",
            yaxis2_title="Ratio Value",
            legend_title_text="Metric & Type",
            
            uirevision=f"hyperopt-{trader_address}-{n_clicks}" 
        )
        elapsed = time.time() - start_time
        logger.info(f"Hyperopt leverage plot generated for {tagname} ({elapsed:.2f}s).")
        
        return fig, fig.to_plotly_json(), base_setup_obj

    except Exception as e:
        elapsed = time.time() - start_time
        logger.exception(f"Error during hyperopt leverage for {trader_address} ({elapsed:.2f}s): {e}")
        fig = go.Figure().update_layout(title_text=f"HyperOpt Error: {e}")
        
        return fig, fig.to_plotly_json(), base_setup_obj



@callback(
    Output("graph-output", "figure", allow_duplicate=True),
    Output("latest-graph-figure", "data", allow_duplicate=True),
    Output("current-iteration-setup", "data", allow_duplicate=True),
    
    Output("drawer-cards-container", "children", allow_duplicate=True),
    
    
    Input({"type": "button-backtest", "index": ALL}, 'n_clicks'),
    State("trader-tag", "value"),           
    State("trader-platform", "value"),      
    State("trader-name", "value"),          
    State("sorting", "value"),              
    prevent_initial_call=True,
    running=[(Output("drawer-simple", "opened", allow_duplicate=True), False, True)] 
)
def backtest_from_card_button(n_clicks_list, new_trader_tag, new_trader_type, new_trader_name, sort_method):
    start_time = time.time()
    triggered_id_dict = ctx.triggered_id
    
    if not triggered_id_dict or not isinstance(triggered_id_dict, dict) or not n_clicks_list or not any(n is not None and n > 0 for n in n_clicks_list):
        logger.debug("Backtest from card button not triggered by click.")
        
        return no_update, no_update, no_update, no_update

    setup_to_run = None
    trader_id_for_log = "N/A"
    is_new_trader_add = triggered_id_dict["index"] == "add-new-trader"
    figure_to_return = no_update
    figure_json_to_store = no_update
    setup_to_store = no_update
    drawer_cards_to_return = no_update 
    drawer_loading = False 


    try:
        
        if is_new_trader_add:
            
            trader_id_for_log = f"NEW: {new_trader_name} ({new_trader_tag})"
            logger.info(f"Attempting to add and backtest new trader: {trader_id_for_log}")
            if not new_trader_tag or not new_trader_type or not new_trader_name:
                logger.warning("Missing information for adding new trader.")
                
                
                raise ValueError("Missing trader name, tag/address, or platform for new trader.")

            
            setup_to_run = [{"Trader Address": new_trader_tag, "type": new_trader_type, "tagname": new_trader_name}]

        else:
            
            trader_address = triggered_id_dict["index"]
            trader_id_for_log = f"EXISTING: {trader_address}"
            logger.info(f"Attempting to backtest existing trader: {trader_id_for_log}")

            
            trader_info_df = backtest_engine.backtest_cacher.obtain_cache_extended('rating').filter(pl.col("Trader Address") == trader_address)
            if trader_info_df.is_empty():
                raise ValueError(f"Trader info not found in cache for backtest: {trader_address}")

            setup_str = trader_info_df.select("setup").item() 
            if not setup_str:
                raise ValueError(f"Setup JSON string missing for trader: {trader_address}")

            try:
                setup_obj = json.loads(setup_str)
                
                setup_to_run = [setup_obj] if isinstance(setup_obj, dict) else setup_obj
                if not isinstance(setup_to_run, list):
                    raise ValueError("Parsed setup must be a list of dicts")
            except json.JSONDecodeError:
                raise ValueError(f"Invalid setup JSON found for trader {trader_address}: {setup_str}")

        
        if not setup_to_run:
            raise ValueError("Setup for backtest could not be determined.")

        logger.info(f"Running backtest for {trader_id_for_log} with setup: {json.dumps(setup_to_run)}")
        backtest_engine.reset_downloaded_queue() 

        
        response = asyncio.run(backtest_engine.backtest_traders(setup_to_run))
        backtest_elapsed = time.time() - start_time
        logger.info(f"Backtest for {trader_id_for_log} completed in {backtest_elapsed:.2f} seconds.")

        
        if response and "graph" in response and isinstance(response["graph"], go.Figure):
            figure = response["graph"]
            figure.update_layout(
                title=f"Backtest Result: {trader_id_for_log}",
                
                uirevision=f"backtest-{triggered_id_dict['index']}-{start_time}"
            )
            figure_to_return = figure
            figure_json_to_store = figure.to_plotly_json()
            setup_to_store = setup_to_run 

            
            logger.info("Backtest successful, refreshing drawer cards...")
            drawer_loading = True 
            
            
            try:
                drawer_cards_to_return = generate_backtest_cards(sort_method)
            except Exception as card_err:
                logger.exception("Failed to regenerate cards after backtest.")
                
                drawer_cards_to_return = no_update 
            drawer_loading = False 

        else:
            logger.error(f"Backtest for {trader_id_for_log} failed or returned no graph.")
            error_fig = go.Figure().update_layout(title_text=f"Backtest Failed: {trader_id_for_log}")
            figure_to_return = error_fig
            figure_json_to_store = error_fig.to_plotly_json()
            
            setup_to_store = setup_to_run
            
            
            drawer_cards_to_return = no_update
            drawer_loading = False


    except Exception as e:
        
        elapsed = time.time() - start_time
        logger.exception(f"Error during backtest from card button ({trader_id_for_log}, {elapsed:.2f}s): {e}")
        error_fig = go.Figure().update_layout(title_text=f"Error: {e}")
        figure_to_return = error_fig
        figure_json_to_store = error_fig.to_plotly_json()
        
        setup_to_store = setup_to_run if 'setup_to_run' in locals() else None
        drawer_cards_to_return = no_update 
        drawer_loading = False


    
    return figure_to_return, figure_json_to_store, setup_to_store, drawer_cards_to_return



@callback(
    Output("graph-output", "figure", allow_duplicate=True),
    Output("latest-graph-figure", "data", allow_duplicate=True),
    Output("current-iteration-setup", "data", allow_duplicate=True),
    Output("drawer-cards-container", "children", allow_duplicate=True),
    
    Input("bt-bt-json", 'n_clicks'),
    State("json-trader-input", "value"),
    State("sorting", "value"),
    prevent_initial_call=True,
    running=[(Output("drawer-simple", "opened", allow_duplicate=True), False, True)] 
)
def backtest_from_json(n_clicks, json_string, sort_method):
    start_time = time.time()
    if not n_clicks or not json_string:
        logger.debug("Backtest from JSON not triggered (no click or empty JSON).")
        return no_update, no_update, no_update, no_update, no_update

    setup_to_run = None
    trader_id_for_log = "JSON Input"
    figure_to_return = no_update
    figure_json_to_store = no_update
    setup_to_store = None 
    drawer_cards_to_return = no_update
    drawer_loading = False

    try:
        
        logger.info("Attempting to backtest from JSON input.")
        try:
            setup_json = json.loads(json_string)
        except json.JSONDecodeError as json_err:
            raise ValueError(f"Invalid JSON format: {json_err}")

        
        if isinstance(setup_json, dict):
            setup_to_run = [setup_json] 
        elif isinstance(setup_json, list):
            setup_to_run = setup_json
        else:
            raise ValueError("JSON input must be a single trader object or a list of trader objects.")

        
        if not setup_to_run or not all(isinstance(item, dict) and "Trader Address" in item and "type" in item for item in setup_to_run):
            raise ValueError("Invalid JSON structure. Each object needs at least 'Trader Address' and 'type'.")

        
        setup_to_store = setup_to_run

        
        trader_ids = [item.get("tagname", item.get("Trader Address", "Unknown")) for item in setup_to_run]
        trader_id_for_log = f"JSON: {', '.join(trader_ids[:3])}{'...' if len(trader_ids)>3 else ''}"

        
        logger.info(f"Running backtest for {trader_id_for_log}")
        backtest_engine.reset_downloaded_queue()
        response = asyncio.run(backtest_engine.backtest_traders(setup_to_run))
        backtest_elapsed = time.time() - start_time
        logger.info(f"Backtest from JSON for {trader_id_for_log} completed in {backtest_elapsed:.2f} seconds.")

        
        if response and "graph" in response and isinstance(response["graph"], go.Figure):
            figure = response["graph"]
            figure.update_layout(
                title=f"Backtest Result: {trader_id_for_log}",
                uirevision=f"backtest-json-{start_time}"
            )
            figure_to_return = figure
            figure_json_to_store = figure.to_plotly_json()
            

            
            logger.info("JSON backtest successful, refreshing drawer cards...")
            drawer_loading = True
            try:
                drawer_cards_to_return = generate_backtest_cards(sort_method)
            except Exception as card_err:
                logger.exception("Failed to regenerate cards after JSON backtest.")
                drawer_cards_to_return = no_update
            drawer_loading = False

        else:
            logger.error(f"Backtest from JSON for {trader_id_for_log} failed or returned no graph.")
            error_fig = go.Figure().update_layout(title_text=f"Backtest Failed: {trader_id_for_log}")
            figure_to_return = error_fig
            figure_json_to_store = error_fig.to_plotly_json()
            
            drawer_cards_to_return = no_update
            drawer_loading = False

    except (ValueError, TypeError) as val_err: 
        elapsed = time.time() - start_time
        logger.error(f"Invalid JSON input ({elapsed:.2f}s): {val_err}")
        error_fig = go.Figure().update_layout(title_text=f"JSON Error: {val_err}")
        figure_to_return = error_fig
        figure_json_to_store = error_fig.to_plotly_json()
        
        drawer_cards_to_return = no_update
        drawer_loading = False

    except Exception as e:
        
        elapsed = time.time() - start_time
        logger.exception(f"Error during backtest from JSON ({trader_id_for_log}, {elapsed:.2f}s): {e}")
        error_fig = go.Figure().update_layout(title_text=f"Execution Error: {e}")
        figure_to_return = error_fig
        figure_json_to_store = error_fig.to_plotly_json()
        
        drawer_cards_to_return = no_update
        drawer_loading = False


    
    return figure_to_return, figure_json_to_store, setup_to_store, drawer_cards_to_return


app = dash_app.server 


def signal_handler(sig: int, frame):
    logger.info(f"Received signal {sig}, shutting down gracefully...")
    
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler) 
signal.signal(signal.SIGTERM, signal_handler) 

if __name__ == "__main__":
    
    host = os.environ.get("HOST", "127.0.0.1")
    try:
        
        port = int(os.environ.get("PORT", sys.argv[1] if len(sys.argv) > 1 else 8050))
    except (IndexError, ValueError):
        port = 8050
        logger.warning("No port argument/env var or invalid value, using default port 8050.")

    
    
    debug_mode = os.environ.get("DASH_DEBUG", "true").lower() == "true"

    logger.info(f"Starting Dash server on http://{host}:{port}/")
    logger.info(f"Debug mode: {debug_mode}")

    
    
    dash_app.run(host=host, port=port, debug=debug_mode)