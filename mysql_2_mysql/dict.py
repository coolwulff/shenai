data_process_dict = {
    '&lt;': '<',
    '&gt;': '>',
    '&apos;': "'",
    '&quot;': '\"',
    ',': '，',
    '(': '（',
    ')': '）',
    ':': '：',
    ' ': ''}

computer_rule_dict = {
    # 1_字段抽取数据累加
    "content_sum": {},
    # 2_字段抽取数据判断
    "if_content": {},
    # 3_字段抽取数据判断累加再判断
    "if_content_sum_if": [],
    # 4_字段互斥、字段互为否定
    "if_no_exist_entity": {},
    # 5_字段存在累加判断
    "if_entity_sum_if": [],
    # 6_一些字段共现，另一些字段互斥
    "exist_and_not_exist": {},
}

extract_rule_dict_operate = {
    "block_way_动脉主干": {
        "replace_words": {},
        "search_with": [["", "阻断肾动脉", ""]],
        "search_without": [["", "", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "block_way_动脉分支": {
        "replace_words": {},
        "search_with": [["", "动脉分支", ""]],
        "search_without": [["", "", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "block_way_动静脉同时": {
        "replace_words": {},
        "search_with": [["", "动静脉同时", ""]],
        "search_without": [["", "", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "block_way_零缺血": {
        "replace_words": {},
        "search_with": [["", "零缺血", ""]],
        "search_without": [["", "", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "block_way_冷缺血": {
        "replace_words": {},
        "search_with": [["", "冰屑", ""]],
        "search_without": [["", "", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "block_time": {
        "replace_words": {
            '.分钟': '分钟', 'min': '分钟', 'mim': '分钟', '分肿': '分钟', '缺血约': '缺血时间', '缺血时间为': '缺血时间',
            '缺血时间约': '缺血时间', '缺血时间：': '缺血时间', '缺血阻断时间约': '缺血时间', '缺血阻断时间': '缺血时间',
            '缺血阻断': '缺血时间'},
        "search_with": [
            ["缺血", r"[\d\.]*", "分钟"],
            ["缺血时间", r"[\d\.]*", "分钟"],
            ["缺血时间", r"[\d\.]*", "钟"],
            ["缺血时间", r"[\d\.]*[^*]?[^*]?[\d\.]*", "秒"],
            ["缺血时间", r"[\d\.]*[^*]?[^*]?[\d\.]*", "分钟"]
        ],
        "search_without": [["", "", ""]],
        "search_region": [["", "", "", 0, ()]],
    }
}

extract_rule_dict = {
    "tumor_size1": {
        "replace_words": {'x': '×', '*': '×'},
        "search_with": [
            ["，", r"[\d\.]*", "×"],
            ["约", r"[\d\.]*", "×"],
            ["大小", r"[\d\.]*", "×"],
            ["为", r"[\d\.]*", "×"],
            ["，约", r"[\d\.]*", "×"],
            ["（", r"[\d\.]*", "×"],
            ["型", r"[\d\.]*", "×"],
            ["级", r"[\d\.]*", "×"],
            ["一枚", r"[\d\.]*", "×"],
            ["最大径", r"[\d\.-]*", "cm"],
            [r"\s", r"[\d\.]*", "×"],
            ["直径", r"[\d\.]*", "×"],
            ["”", r"[\d\.]*", "×"],
            ["肿块样物", r"[\d\.]*", "×"]
        ],
        "search_without": [["", "", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "tumor_size2": {
        "replace_words": {'x': '×', '*': '×'},
        "search_with": [
            ["×", r"[\d\.]*", "×"]],
        "search_without": [["", "", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "tumor_size3": {
        "replace_words": {'x': '×', '*': '×'},
        "search_with": [
            ["×", r"[\d\.]+", "cm"]],
        "search_without": [["", "", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "pathology_pattern_透明细胞": {
        "replace_words": {},
        "search_with": [["", "透明细胞", ""]],
        "search_without": [["", "", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "pathology_pattern_乳头状I型": {
        "replace_words": {'Ⅰ': 'I', },
        "search_with": [["I型", "乳头状", ""]],
        "search_without": [["", "", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "pathology_pattern_乳头状II型": {
        "replace_words": {'Ⅱ': 'II', },
        "search_with": [["II型", "乳头状", ""]],
        "search_without": [["", "", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "pathology_pattern_嫌色细胞": {
        "replace_words": {},
        "search_with": [["", "嫌色细胞", ""]],
        "search_without": [["", "", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "pathology_pattern_other_XP11.2": {
        "replace_words": {},
        "search_with": [["", "XP11.2", ""]],
        "search_without": [["", "", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "pathology_pattern_other_嗜酸细胞瘤": {
        "replace_words": {},
        "search_with": [["", "嗜酸细胞瘤", ""]],
        "search_without": [["", "", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "pathology_pattern_other_MiTF家族易位性肾细胞癌": {
        "replace_words": {},
        "search_with": [["", "MiTF家族易位性肾细胞癌", ""]],
        "search_without": [["", "", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "pathology_pattern_other_未分类的肾细胞癌": {
        "replace_words": {},
        "search_with": [["", "未分类的肾细胞癌", ""]],
        "search_without": [["", "", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "pathology_pattern_other_透明细胞乳头状肾细胞癌": {
        "replace_words": {},
        "search_with": [["", "透明细胞乳头状肾细胞癌", ""]],
        "search_without": [["", "", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "pathology_pattern_other_遗传性平滑肌瘤病和肾细胞癌综合征相关性肾细胞癌": {
        "replace_words": {},
        "search_with": [["", "遗传性平滑肌瘤病和肾细胞癌综合征相关性肾细胞癌", ""]],
        "search_without": [["", "", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "pathology_pattern_other_低度恶性潜能多房囊性肾细胞肿瘤": {
        "replace_words": {},
        "search_with": [["", "低度恶性潜能多房囊性肾细胞肿瘤", ""]],
        "search_without": [["", "", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "pathology_pattern_other_集合管癌": {
        "replace_words": {},
        "search_with": [["", "集合管癌", ""]],
        "search_without": [["", "", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "pathology_pattern_other_肾髓质癌": {
        "replace_words": {},
        "search_with": [["", "肾髓质癌", ""]],
        "search_without": [["", "", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "pathology_pattern_other_琥珀酸脱氢酶缺陷相关的肾细胞癌": {
        "replace_words": {},
        "search_with": [["", "琥珀酸脱氢酶缺陷相关的肾细胞癌", ""]],
        "search_without": [["", "", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "pathology_pattern_other_黏液性管状和梭形细胞癌": {
        "replace_words": {},
        "search_with": [["", "黏液性管状和梭形细胞癌", ""]],
        "search_without": [["", "", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "pathology_pattern_other_管状囊性肾细胞癌": {
        "replace_words": {},
        "search_with": [["", "管状囊性肾细胞癌", ""]],
        "search_without": [["", "", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "pathology_pattern_other_获得性囊性肾病相关性肾细胞癌": {
        "replace_words": {},
        "search_with": [["", "获得性囊性肾病相关性肾细胞癌", ""]],
        "search_without": [["", "", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "pathology_pattern_other_乳头状腺瘤": {
        "replace_words": {},
        "search_with": [["", "乳头状腺瘤", ""]],
        "search_without": [["", "", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "pathology_pattern_other_VHL综合征相关性肾癌": {
        "replace_words": {},
        "search_with": [["", "VHL综合征相关性肾癌", ""]],
        "search_without": [["", "", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "tumor_status_肉瘤样变": {
        "replace_words": {},
        "search_with": [["伴", "肉瘤样变", ""]],
        "search_without": [["", "", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "tumor_status_坏死": {
        "replace_words": {},
        "search_with": [["伴", "坏死", ""]],
        "search_without": [["", "", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "tumor_status_淋巴转移": {
        "replace_words": {},
        "search_with": [["淋巴结", "癌组织转移", ""]],
        "search_without": [["", "", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "tumor_status_癌栓": {
        "replace_words": {},
        "search_with": [["", "癌栓", ""]],
        "search_without": [["", "未见", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "tumor_status_累及肾包膜": {
        "replace_words": {},
        "search_with": [["侵犯", "肾包膜", ""], ["累及", "肾包膜", ""], ["浸润", "肾包膜", ""]],
        "search_without": [["", "未", ""], ["", "阴性", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "tumor_status_累及肾窦脂肪": {
        "replace_words": {},
        "search_with": [["侵犯", "肾窦脂肪", ""], ["累及", "肾窦脂肪", ""], ["浸润", "肾窦脂肪", ""]],
        "search_without": [["", "未", ""], ["", "阴性", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "tumor_status_累及肾周脂肪": {
        "replace_words": {},
        "search_with": [["侵犯", "肾周脂肪", ""], ["累及", "肾周脂肪", ""], ["浸润", "肾周脂肪", ""]],
        "search_without": [["", "未", ""], ["", "阴性", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "tumor_status_累及集合系统": {
        "replace_words": {},
        "search_with": [["侵犯", "集合系统", ""], ["累及", "集合系统", ""], ["浸润", "集合系统", ""]],
        "search_without": [["", "未", ""], ["", "阴性", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "tumor_status_累及肾上腺": {
        "replace_words": {},
        "search_with": [["侵犯", "肾上腺", ""], ["累及", "肾上腺", ""], ["浸润", "肾上腺", ""]],
        "search_without": [["", "未", ""], ["", "阴性", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "tumor_status_出血": {
        "replace_words": {},
        "search_with": [["伴", "出血", ""]],
        "search_without": [["", "", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "who_isup_I级": {
        "replace_words": {'Ⅰ': 'I', },
        "search_with": [
            ["癌", "I级", ""], ["癌，", "I级", ""]],
        "search_without": [["", "", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "who_isup_I-II级": {
        "replace_words": {'Ⅰ-Ⅱ': 'I-II', },
        "search_with": [
            ["癌", "I-II级", ""], ["癌，", "I-II级", ""]],
        "search_without": [["", "", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "who_isup_II级": {
        "replace_words": {'Ⅱ': 'II', },
        "search_with": [
            ["癌", "II级", ""], ["，", "II级", ""],
            ["胞", "II级", ""], ["胞，", "II级", ""],
            ["（", "II级", ""]],
        "search_without": [["", "", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "who_isup_II-III级": {
        "replace_words": {'Ⅱ-Ⅲ': 'II-III', 'II－III': 'II-III', },
        "search_with": [
            ["癌", "II-III级", ""], ["癌，", "II-III级", ""],
            ["癌", "Ⅱ-Ⅲ级", ""], ["癌，", "Ⅱ-Ⅲ级", ""],
            ["癌", "II－III级", ""], ["癌，", "II－III级", ""]],
        "search_without": [["", "", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "who_isup_III级": {
        "replace_words": {'Ⅲ': 'III', },
        "search_with": [
            ["癌", "III级", ""], ["，", "III级", ""],
            ["癌2型，", "Ⅲ级", ""], ["（", "III级", ""]],
        "search_without": [["", "", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "who_isup_III-IV级": {
        "replace_words": {'Ⅲ-Ⅳ': 'III-IV', },
        "search_with": [
            ["癌", "III-IV级", ""], ["癌，", "III-IV级", ""]],
        "search_without": [["", "", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
    "who_isup_IV级": {
        "replace_words": {'Ⅳ': 'IV', },
        "search_with": [
            ["癌", "IV级", ""], ["，", "IV级", ""], ["（", "IV级", ""]],
        "search_without": [["", "", ""]],
        "search_region": [["", "", "", 0, ()]],
    },
}

