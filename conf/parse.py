

title_dic = {

    '信用額度總量管制餘額表': ['信用額度總量管制餘額表'],
    '當日沖銷交易標的及成交量值': ["當日沖銷交易標的及成交量值", "當日沖銷交易統計資訊"],
    '每月當日沖銷交易標的及統計': ['每月當日沖銷交易標的及統計'],
    '外資及陸資投資持股統計': ["外資及陸資投資持股統計", "外資投資持股統計"],
    '發行量加權股價指數歷史資料': ['發行量加權股價指數歷史資料']
}

data_dic = {
    "每日收盤行情": {
        "m": "market",
        "freq": "D",
        'date_name': "date",
        "date_min": "2004-2-11",
        "url": r"https://www.twse.com.tw/exchangeReport/MI_INDEX?",
        'sub_item': ["價格指數(臺灣證券交易所)", "價格指數(跨市場)", "價格指數(臺灣指數公司)", "報酬指數(臺灣證券交易所)", "報酬指數(跨市場)", "報酬指數(臺灣指數公司)", "大盤統計資訊", "漲跌證券數合計", "每日收盤行情"],
        "payload": {
            "response": "json",
            "date": "",
            "type": "ALL",
            "_": "1613296592078"
        },
    },
    "信用交易統計": {
        "m": "market",
        "freq": "D",
        'date_name': "date",
        "date_min": "2001-1-1",
        "url": r"https://www.twse.com.tw/exchangeReport/MI_MARGN?",
        'sub_item': ["融資融券彙總", "信用交易統計"],
        "payload": {
            "response": "json",
            "date": "",
            "selectType": "ALL"
        },
    },
    "市場成交資訊": {
        "m": "market",
        "freq": "M",
        "date_min": "1990-1-4",
        'date_name': "date",
        "url": r"https://www.twse.com.tw/exchangeReport/FMTQIK?",
        'sub_item': ['市場成交資訊'],
        "payload": {
            "response": "json",
            "date": "",
            "_": "1613392395864"
        },
    },
    "三大法人買賣金額統計表": {
        "m": "market",
        "freq": "D",
        "date_min": "2004-4-7",
        'date_name': "dayDate",
        "url": r"https://www.twse.com.tw/fund/BFI82U?",
        'sub_item': ['三大法人買賣金額統計表'],
        "payload": {
            "response": "json",
            "dayDate": "",
            "type": "day",
            "_": "1613389589646"
        },
    },
    "三大法人買賣超日報": {
        "m": "market",
        "freq": "D",
        "date_min": "2012-5-2",
        'date_name': "date",
        "url": r"https://www.twse.com.tw/fund/T86?",
        'sub_item': ["三大法人買賣超日報"],
        "payload": {
            "response": "json",
            "date": "",
            "selectType": "ALL"
        },
    },
    "個股日本益比、殖利率及股價淨值比": {
        "m": "stock",
        "freq": "D",
        "date_min": "2012-5-2",
        'date_name': "date",
        "url": r"https://www.twse.com.tw/exchangeReport/BWIBBU_d?",
        'sub_item': ['個股日本益比、殖利率及股價淨值比'],
        "payload": {
            "response": "json",
            "date": "",
            "selectType": "ALL",
            "_": "1596117278906"
        },
    },
    "信用額度總量管制餘額表": {
        "m": "stock",
        "freq": "D",
        "date_min": "2005-7-1",
        'date_name': "date",
        "url": r"https://www.twse.com.tw/exchangeReport/TWT93U?",
        "payload": {
            "response": "json",
            "date": "",
            "_": "1596721575815"
        },
    },
    "當日沖銷交易標的及成交量值": {
        "m": "stock",
        "freq": "D",
        "date_min": "2014-1-6",
        'date_name': "date",
        "url": r"https://www.twse.com.tw/exchangeReport/TWTB4U?",
        "payload": {
            "response": "json",
            "date": "",
            "selectType": "All",
            "_": "1596117305431"
        },
    },
    # 這裡的,"當日沖銷交易統計"跟market有重複，因為都是大盤的沖銷交易===========
    "每月當日沖銷交易標的及統計": {
        "m": "market",
        "freq": "M",
        "date_min": "2014-1-6",
        'date_name': "date",
        "url": "https://www.twse.com.tw/exchangeReport/TWTB4U2?",
        "payload": {
            "response": "json",
            "date": "",
            "stockNo": "",
            "_": "1596117360962"
        },
    },
    "外資及陸資投資持股統計": {
        "m": "stock",
        "freq": "D",
        "date_min": "2004-2-11",
        'date_name': "date",
        "url": "https://www.twse.com.tw/fund/MI_QFIIS?",
        "payload": {
            "response": "json",
            "date": "",
            "selectType": "ALLBUT0999",
            "_": "1594606204191"
        },
    },
    "發行量加權股價指數歷史資料": {
        "m": "market",
        "freq": "D",
        "date_min": "1999-1-5",
        'date_name': "date",
        "url": "https://www.twse.com.tw/indicesReport/MI_5MINS_HIST?",
        "payload": {
            "response": "json",
            "date": "",
            "_": "1597539490294"
        },
    },
    'stock': {
        "url": r"https://isin.twse.com.tw/isin/C_public.jsp?strMode={}",
        "charset": "cp950"

    }
}