# -----------------------
# 算法的参数
DEFAULT_ASSET_WEIGHTS = {
    '存货': {'codes': ['001001031', '003002001', '004002001'], 'CashOnlyAsset': 0.0, 'ConservativeAsset': 0.05, 'NormalAsset': 0.15},
    '发展中及待售物业': {'codes': ['003002002', '004002002'], 'CashOnlyAsset': 0.0, 'ConservativeAsset': 0.03, 'NormalAsset': 0.1},
    '应收帐款': {'codes': ['003002003', '004002003'], 'CashOnlyAsset': 0.0, 'ConservativeAsset': 0.25, 'NormalAsset': 0.55},
    '应收票据': {'codes': ['003002004', '004002004'], 'CashOnlyAsset': 0.1, 'ConservativeAsset': 0.45, 'NormalAsset': 0.7},
    '预付款按金及其他应收款': {'codes': ['001001028', '002001027', '003002005', '004002005'], 'CashOnlyAsset': 0.0,
                               'ConservativeAsset': 0.2, 'NormalAsset': 0.4},
    '应收关联方款项': {'codes': ['003002006', '004002006'], 'CashOnlyAsset': 0.0, 'ConservativeAsset': 0.05, 'NormalAsset': 0.2},
    '预缴及应收税项': {'codes': ['003002007', '004002007'], 'CashOnlyAsset': 0.5, 'ConservativeAsset': 0.7, 'NormalAsset': 0.9},
    '短期投资': {'codes': ['003002008', '004002008'], 'CashOnlyAsset': 0.7, 'ConservativeAsset': 0.8, 'NormalAsset': 0.9},
    '受限制存款及现金': {'codes': ['002001018', '003002009', '004002009'], 'CashOnlyAsset': 0.5, 'ConservativeAsset': 0.7,
                         'NormalAsset': 0.9},
    '现金及等价物': {'codes': ['003002010', '004002010'], 'CashOnlyAsset': 1.0, 'ConservativeAsset': 1.0, 'NormalAsset': 1.0},
    '短期存款': {'codes': ['003002011', '004002011'], 'CashOnlyAsset': 0.9, 'ConservativeAsset': 0.9, 'NormalAsset': 1.0},
    '其他流动资产': {'codes': ['003002015', '004002012'], 'CashOnlyAsset': 0.0, 'ConservativeAsset': 0.05, 'NormalAsset': 0.1},
    '指定以公允价值记账之金融资产(流动)': {'codes': ['003002014', '004002013'], 'CashOnlyAsset': 0.6, 'ConservativeAsset': 0.7,
                                           'NormalAsset': 0.8},
    '交易性金融资产(流动)': {'codes': ['003002016', '004002014'], 'CashOnlyAsset': 0.65, 'ConservativeAsset': 0.75, 'NormalAsset': 0.85},
    '衍生金融工具-资产(流动)': {'codes': ['003002017', '004002015'], 'CashOnlyAsset': 0.2, 'ConservativeAsset': 0.2, 'NormalAsset': 0.4},
    '持有至到期投资(流动)': {'codes': ['003002018', '004002016'], 'CashOnlyAsset': 0.8, 'ConservativeAsset': 0.8, 'NormalAsset': 0.9},
    '可供出售投资(流动)': {'codes': ['003002019', '004002017'], 'CashOnlyAsset': 0.5, 'ConservativeAsset': 0.5, 'NormalAsset': 0.7},
    '持作出售的资产(流动)': {'codes': ['004002018'], 'CashOnlyAsset': 0.1, 'ConservativeAsset': 0.1, 'NormalAsset': 0.2},
    '递延税项资产(流动)': {'codes': ['004002019'], 'CashOnlyAsset': 0.0, 'ConservativeAsset': 0.05, 'NormalAsset': 0.1},
    '贷款及垫款(流动)': {'codes': ['004002020'], 'CashOnlyAsset': 0.0, 'ConservativeAsset': 0.2, 'NormalAsset': 0.3},
    '生产性生物资产': {'codes': ['004002021'], 'CashOnlyAsset': 0.0, 'ConservativeAsset': 0.1, 'NormalAsset': 0.2},
    '其他金融资产(流动)': {'codes': ['004002022'], 'CashOnlyAsset': 0.1, 'ConservativeAsset': 0.2, 'NormalAsset': 0.3},
    '合同资产': {'codes': ['004002023'], 'CashOnlyAsset': 0.0, 'ConservativeAsset': 0.1, 'NormalAsset': 0.2},
    '流动资产其他项目': {'codes': ['003002997', '004002997'], 'CashOnlyAsset': 0.0, 'ConservativeAsset': 0.0, 'NormalAsset': 0.0},
    '流动资产平衡项目': {'codes': ['004002998'], 'CashOnlyAsset': 0.0, 'ConservativeAsset': 0.0, 'NormalAsset': 0.0},
    '固定资产': {'codes': ['001001014', '003001001', '004001001'], 'CashOnlyAsset': 0.0, 'ConservativeAsset': 0.1, 'NormalAsset': 0.2},
    '物业厂房及设备': {'codes': ['003001002', '004001002'], 'CashOnlyAsset': 0.0, 'ConservativeAsset': 0.1, 'NormalAsset': 0.2},
    '投资物业': {'codes': ['001001012', '002001025', '003001003', '004001003'], 'CashOnlyAsset': 0.0, 'ConservativeAsset': 0.2,
                 'NormalAsset': 0.3},
    '无形资产': {'codes': ['001001015', '002001004', '003001004', '004001004'], 'CashOnlyAsset': 0.0, 'ConservativeAsset': 0.0,
                 'NormalAsset': 0.0},
    '商誉': {'codes': ['001001016', '002001003', '003001005', '004001005'], 'CashOnlyAsset': 0.0, 'ConservativeAsset': 0.0,
             'NormalAsset': 0.0},
    '负商誉': {'codes': ['003001006', '004001006'], 'CashOnlyAsset': 0.0, 'ConservativeAsset': 0.0, 'NormalAsset': 0.0},
    '土地使用权': {'codes': ['001001017', '002001016', '003001007', '004001007'], 'CashOnlyAsset': 0.0, 'ConservativeAsset': 0.25,
                   'NormalAsset': 0.4},
    '在建工程': {'codes': ['003001008', '004001008'], 'CashOnlyAsset': 0.0, 'ConservativeAsset': 0.0, 'NormalAsset': 0.1},
    '递延税项资产': {'codes': ['001001013', '003001009', '004001009'], 'CashOnlyAsset': 0.0, 'ConservativeAsset': 0.02,
                     'NormalAsset': 0.05},
    '预付款项': {'codes': ['003001010', '004001010'], 'CashOnlyAsset': 0.0, 'ConservativeAsset': 0.05, 'NormalAsset': 0.15},
    '长期应收款': {'codes': ['003001011', '004001011'], 'CashOnlyAsset': 0.0, 'ConservativeAsset': 0.15, 'NormalAsset': 0.3},
    '开发支出': {'codes': ['003001012', '004001012'], 'CashOnlyAsset': 0.0, 'ConservativeAsset': 0.0, 'NormalAsset': 0.0},
    '联营公司权益': {'codes': ['001001008', '002001005', '003001013', '004001013'], 'CashOnlyAsset': 0.0,
                     'ConservativeAsset': 0.2, 'NormalAsset': 0.4},
    '附属公司权益': {'codes': ['002001026', '003001015', '004001015'], 'CashOnlyAsset': 0.0, 'ConservativeAsset': 0.15,
                     'NormalAsset': 0.35},
    '合营公司权益': {'codes': ['001001029', '003001016', '004001016'], 'CashOnlyAsset': 0.0, 'ConservativeAsset': 0.15,
                     'NormalAsset': 0.35},
    '长期投资': {'codes': ['003001017', '004001017'], 'CashOnlyAsset': 0.0, 'ConservativeAsset': 0.2, 'NormalAsset': 0.4},
    '证券投资': {'codes': ['001001010', '003001018', '004001018'], 'CashOnlyAsset': 0.2, 'ConservativeAsset': 0.4, 'NormalAsset': 0.6},
    '其他投资': {'codes': ['002001009', '003001022', '004001019'], 'CashOnlyAsset': 0.0, 'ConservativeAsset': 0.0, 'NormalAsset': 0.0},
    '其他非流动资产': {'codes': ['003001023', '004001020'], 'CashOnlyAsset': 0.0, 'ConservativeAsset': 0.05, 'NormalAsset': 0.15},
    '交易性金融资产': {'codes': ['002001014', '003001024', '004001021'], 'CashOnlyAsset': 0.4, 'ConservativeAsset': 0.5,
                       'NormalAsset': 0.6},
    '指定以公允价值记账之金融资产': {'codes': ['002001024', '003001021', '004001022'], 'CashOnlyAsset': 0.4,
                                     'ConservativeAsset': 0.5, 'NormalAsset': 0.6},
    '衍生金融工具-资产': {'codes': ['001001019', '003001025', '004001023'], 'CashOnlyAsset': 0.1, 'ConservativeAsset': 0.15,
                          'NormalAsset': 0.3},
    '持有至到期投资': {'codes': ['001001020', '004001024'], 'CashOnlyAsset': 0.5, 'ConservativeAsset': 0.6, 'NormalAsset': 0.8},
    '买入返售金融资产': {'codes': ['001001022', '002001020', '004001025'], 'CashOnlyAsset': 0.6, 'ConservativeAsset': 0.7,
                         'NormalAsset': 0.85},
    '贷款及垫款': {'codes': ['003002013', '004001026'], 'CashOnlyAsset': 0.0, 'ConservativeAsset': 0.1, 'NormalAsset': 0.2},
    '可供出售投资': {'codes': ['001001024', '003001019', '004001027'], 'CashOnlyAsset': 0.25, 'ConservativeAsset': 0.35,
                     'NormalAsset': 0.55},
    '长期待摊费用': {'codes': ['004001028'], 'CashOnlyAsset': 0.0, 'ConservativeAsset': 0.0, 'NormalAsset': 0.0},
    '现金及等价物(非流动)': {'codes': ['004001029'], 'CashOnlyAsset': 0.4, 'ConservativeAsset': 0.6, 'NormalAsset': 0.85},
    '中长期存款': {'codes': ['004001030'], 'CashOnlyAsset': 0.7, 'ConservativeAsset': 0.85, 'NormalAsset': 0.95},
    '其他金融资产(非流动)': {'codes': ['004001031'], 'CashOnlyAsset': 0.05, 'ConservativeAsset': 0.1, 'NormalAsset': 0.2},
    '于联营公司可赎回工具的投资': {'codes': ['004001032'], 'CashOnlyAsset': 0.1, 'ConservativeAsset': 0.2, 'NormalAsset': 0.4},
    '非流动资产其他项目': {'codes': ['003001997', '004001997'], 'CashOnlyAsset': 0.0, 'ConservativeAsset': 0.0, 'NormalAsset': 0.0},
    '非流动资产平衡项目': {'codes': ['004001998'], 'CashOnlyAsset': 0.0, 'ConservativeAsset': 0.0, 'NormalAsset': 0.0},
    # ------------------------
    # 下面是非004开头的内容
    '现金及现金等价物': {'codes': ['002001001'], 'CashOnlyAsset': 1.00, 'ConservativeAsset': 1.00, 'NormalAsset': 1.00},
    '库存现金及短期资金': {'codes': ['001001001'], 'CashOnlyAsset': 0.90, 'ConservativeAsset': 0.95, 'NormalAsset': 1.00},
    '存放中央银行款项': {'codes': ['001001030'], 'CashOnlyAsset': 0.60, 'ConservativeAsset': 0.80, 'NormalAsset': 0.95},
    '银行同业及其他金融机构存款(资产)': {'codes': ['001001002'], 'CashOnlyAsset': 0.60, 'ConservativeAsset': 0.80, 'NormalAsset': 0.95},
    '结算备付金': {'codes': ['003002023'], 'CashOnlyAsset': 0.50, 'ConservativeAsset': 0.70, 'NormalAsset': 0.90},
    '法定存款': {'codes': ['002001017'], 'CashOnlyAsset': 0.30, 'ConservativeAsset': 0.60, 'NormalAsset': 0.80},
    '存款': {'codes': ['003001028'], 'CashOnlyAsset': 0.80, 'ConservativeAsset': 0.90, 'NormalAsset': 1.00},
    '定期存款': {'codes': ['002001019'], 'CashOnlyAsset': 0.70, 'ConservativeAsset': 0.85, 'NormalAsset': 0.95},
    '香港特区政府负债证明书': {'codes': ['001001006'], 'CashOnlyAsset': 0.90, 'ConservativeAsset': 0.95, 'NormalAsset': 1.00},
    '买入返售金融资产(流动)': {'codes': ['003002024'], 'CashOnlyAsset': 0.60, 'ConservativeAsset': 0.75, 'NormalAsset': 0.90},
    '买入返售金融资产(非流动)': {'codes': ['003001027'], 'CashOnlyAsset': 0.50, 'ConservativeAsset': 0.70, 'NormalAsset': 0.85},
    '交易用途资产': {'codes': ['001001004'], 'CashOnlyAsset': 0.40, 'ConservativeAsset': 0.50, 'NormalAsset': 0.60},
    '以公允价值计量的金融资产': {'codes': ['001001005'], 'CashOnlyAsset': 0.40, 'ConservativeAsset': 0.50, 'NormalAsset': 0.60},
    '其他短期投资': {'codes': ['001001023'], 'CashOnlyAsset': 0.60, 'ConservativeAsset': 0.80, 'NormalAsset': 0.90},
    '其他证券投资': {'codes': ['001001011'], 'CashOnlyAsset': 0.30, 'ConservativeAsset': 0.45, 'NormalAsset': 0.65},
    '可供出售证券': {'codes': ['002001006'], 'CashOnlyAsset': 0.30, 'ConservativeAsset': 0.45, 'NormalAsset': 0.65},
    '持有至到期证券': {'codes': ['002001007'], 'CashOnlyAsset': 0.50, 'ConservativeAsset': 0.60, 'NormalAsset': 0.80},
    '持有至到期的投资': {'codes': ['003001020'], 'CashOnlyAsset': 0.50, 'ConservativeAsset': 0.60, 'NormalAsset': 0.80},
    '贵金属': {'codes': ['001001032'], 'CashOnlyAsset': 0.30, 'ConservativeAsset': 0.50, 'NormalAsset': 0.70},
    '衍生金融工具': {'codes': ['002001029'], 'CashOnlyAsset': 0.10, 'ConservativeAsset': 0.15, 'NormalAsset': 0.30},
    '应收款项类投资': {'codes': ['001001026'], 'CashOnlyAsset': 0.20, 'ConservativeAsset': 0.40, 'NormalAsset': 0.60},
    '拆出资金': {'codes': ['001001021', '003002025'], 'CashOnlyAsset': 0.20, 'ConservativeAsset': 0.40, 'NormalAsset': 0.60},
    '贸易票据': {'codes': ['001001003'], 'CashOnlyAsset': 0.10, 'ConservativeAsset': 0.30, 'NormalAsset': 0.50},
    '保证金': {'codes': ['003002022'], 'CashOnlyAsset': 0.20, 'ConservativeAsset': 0.40, 'NormalAsset': 0.60},
    '应收投资收益': {'codes': ['002001023'], 'CashOnlyAsset': 0.10, 'ConservativeAsset': 0.40, 'NormalAsset': 0.60},
    '应收保费': {'codes': ['002001010'], 'CashOnlyAsset': 0.00, 'ConservativeAsset': 0.20, 'NormalAsset': 0.40},
    '可收回税项': {'codes': ['001001027', '002001021', '003002021'], 'CashOnlyAsset': 0.50, 'ConservativeAsset': 0.70,
                   'NormalAsset': 0.90},
    '客户垫款': {'codes': ['003002012'], 'CashOnlyAsset': 0.00, 'ConservativeAsset': 0.15, 'NormalAsset': 0.30},
    '客户贷款及其他款项': {'codes': ['001001007'], 'CashOnlyAsset': 0.00, 'ConservativeAsset': 0.15, 'NormalAsset': 0.30},
    '客户贷款及垫款净额': {'codes': ['002001008'], 'CashOnlyAsset': 0.00, 'ConservativeAsset': 0.20, 'NormalAsset': 0.40},
    '贷款及垫款(非流动)': {'codes': ['003001026'], 'CashOnlyAsset': 0.00, 'ConservativeAsset': 0.10, 'NormalAsset': 0.20},
    '再保险资产': {'codes': ['002001012'], 'CashOnlyAsset': 0.00, 'ConservativeAsset': 0.10, 'NormalAsset': 0.30},
    '保险合同资产': {'codes': ['002001028'], 'CashOnlyAsset': 0.00, 'ConservativeAsset': 0.05, 'NormalAsset': 0.15},
    '递延保单获得成本': {'codes': ['002001015'], 'CashOnlyAsset': 0.00, 'ConservativeAsset': 0.00, 'NormalAsset': 0.00},
    '递延所得税资产': {'codes': ['002001011'], 'CashOnlyAsset': 0.00, 'ConservativeAsset': 0.02, 'NormalAsset': 0.05},
    '于合营公司投资': {'codes': ['002001022'], 'CashOnlyAsset': 0.00, 'ConservativeAsset': 0.15, 'NormalAsset': 0.35},
    '附属公司及其他权益': {'codes': ['001001009'], 'CashOnlyAsset': 0.00, 'ConservativeAsset': 0.15, 'NormalAsset': 0.35},
    '其他长期投资': {'codes': ['001001025'], 'CashOnlyAsset': 0.00, 'ConservativeAsset': 0.10, 'NormalAsset': 0.25},
    '固定资产投资': {'codes': ['002001002'], 'CashOnlyAsset': 0.00, 'ConservativeAsset': 0.05, 'NormalAsset': 0.15},
    '其他资产': {'codes': ['001001018', '002001013'], 'CashOnlyAsset': 0.00, 'ConservativeAsset': 0.05, 'NormalAsset': 0.10},
    '客户信托存款': {'codes': ['003002020'], 'CashOnlyAsset': 0.00, 'ConservativeAsset': 0.00, 'NormalAsset': 0.00},
}




# -----------------------
# 负债权重（两口径）
#   TotalLiab: 总负债口径（只用“总负债”一行，避免重复）
#   PriorityLiab: 紧迫/优先现金覆盖口径,
#       如果负债是有息负债, 权重为1
#       如果还款的强制性, 时间越紧迫, 权重越大
# -----------------------
DEFAULT_LIAB_WEIGHTS = {
    # ---- 流动负债明细（更紧迫）----
    '应付帐款': {'codes': ['004011001'], 'TotalLiab': 0.0, 'PriorityLiab': 0.30},
    '应付票据': {'codes': ['004011002', '003007002'], 'TotalLiab': 0.0, 'PriorityLiab': 0.50},
    '应付税项': {'codes': ['004011003', '003007003'], 'TotalLiab': 0.0, 'PriorityLiab': 0.98},
    '应付股利': {'codes': ['004011004', '003007004'], 'TotalLiab': 0.0, 'PriorityLiab': 0.90},
    '应付关联方款项(流动)': {'codes': ['004011005', '003007005'], 'TotalLiab': 0.0, 'PriorityLiab': 0.40},
    '融资租赁负债(流动)': {'codes': ['004011006', '003007006'], 'TotalLiab': 0.0, 'PriorityLiab': 0.90},
    '递延收入(流动)': {'codes': ['004011007', '003007007'], 'TotalLiab': 0.0, 'PriorityLiab': 0.10},
    '其他应付款及应计费用': {'codes': ['004011008', '003007008'], 'TotalLiab': 0.0, 'PriorityLiab': 0.50},
    '预收款项': {'codes': ['004011009', '003007009'], 'TotalLiab': 0.0, 'PriorityLiab': 0.10},
    '短期贷款': {'codes': ['004011010'], 'TotalLiab': 0.0, 'PriorityLiab': 1.00},
    '拨备(流动)': {'codes': ['004011012', '003007012'], 'TotalLiab': 0.0, 'PriorityLiab': 0.55},
    '其他流动负债': {'codes': ['004011013', '003007014'], 'TotalLiab': 0.0, 'PriorityLiab': 0.50},
    '交易性金融负债(流动)': {'codes': ['004011014', '003007015'], 'TotalLiab': 0.0, 'PriorityLiab': 0.80},
    '指定以公允价值记账之金融负债(流动)': {'codes': ['004011015', '003007013'], 'TotalLiab': 0.0, 'PriorityLiab': 0.80},
    '衍生金融工具-负债(流动)': {'codes': ['004011016', '003007016'], 'TotalLiab': 0.0, 'PriorityLiab': 0.80},
    '持作出售的负债(流动)': {'codes': ['004011017'], 'TotalLiab': 0.0, 'PriorityLiab': 0.30},
    '财务担保合同及负债(流动)': {'codes': ['004011018'], 'TotalLiab': 0.0, 'PriorityLiab': 0.70},
    '职工薪酬及福利(流动)': {'codes': ['004011019', '003007017'], 'TotalLiab': 0.0, 'PriorityLiab': 0.90},
    '递延税项负债(流动)': {'codes': ['004011020'], 'TotalLiab': 0.0, 'PriorityLiab': 0.05},

    # 银行/金融机构特有项（若你筛烟蒂非金融，可后续整体降权或按行业分支）
    '应付债券': {'codes': ['004011021'], 'TotalLiab': 0.0, 'PriorityLiab': 1.00},
    '吸收存款及同业存放': {'codes': ['004011022'], 'TotalLiab': 0.0, 'PriorityLiab': 1.00},
    '其他金融负债(流动)': {'codes': ['004011023'], 'TotalLiab': 0.0, 'PriorityLiab': 0.60},

    # 合同负债：持续经营下不一定要“现金偿还”，更多是交付义务
    '合同负债': {'codes': ['004011024'], 'TotalLiab': 0.0, 'PriorityLiab': 0.10},

    # ---- 汇总/派生项：默认 0（避免重复计数）----
    # '流动负债其他项目': {'codes': ['004011997', '003007997'], 'TotalLiab': 0.0, 'PriorityLiab': 0.00},
    # '流动负债合计': {'codes': ['004011999', '003007999'], 'TotalLiab': 0.0, 'PriorityLiab': 0.00},
    # '净流动资产': {'codes': ['004013999', '003009001'], 'TotalLiab': 0.0, 'PriorityLiab': 0.00},
    # '总资产减流动负债': {'codes': ['004015999', '003011001'], 'TotalLiab': 0.0, 'PriorityLiab': 0.00},
    # '总资产减总负债合计': {'codes': ['004016999'], 'TotalLiab': 0.0, 'PriorityLiab': 0.00},

    # ---- 非流动负债明细（紧迫性通常低于流动端）----
    '长期贷款': {'codes': ['004020001'], 'TotalLiab': 0.0, 'PriorityLiab': 1.00},
    '递延税项负债': {'codes': ['004020003', '001002007', '002002014', '003015003'], 'TotalLiab': 0.0, 'PriorityLiab': 0.05},
    '应付关联方款项(非流动)': {'codes': ['004020004', '003015004'], 'TotalLiab': 0.0, 'PriorityLiab': 0.30},
    '融资租赁负债(非流动)': {'codes': ['004020005', '003015005'], 'TotalLiab': 0.0, 'PriorityLiab': 0.80},
    '递延收入(非流动)': {'codes': ['004020006', '003015006'], 'TotalLiab': 0.0, 'PriorityLiab': 0.05},
    '可转换票据及债券': {'codes': ['004020007', '003015007'], 'TotalLiab': 0.0, 'PriorityLiab': 0.90},
    '拨备(非流动)': {'codes': ['004020008', '003015008'], 'TotalLiab': 0.0, 'PriorityLiab': 0.45},
    '长期应付款': {'codes': ['004020009', '003015009'], 'TotalLiab': 0.0, 'PriorityLiab': 0.25},
    '其他非流动负债': {'codes': ['004020010', '003015010'], 'TotalLiab': 0.0, 'PriorityLiab': 0.50},

    # 非流动“金融负债/公允价值/衍生”：多与市场波动、保证金、净额结算相关；现金压力不稳定
    '交易性金融负债': {'codes': ['004020011', '001002006', '002002006', '003015011'], 'TotalLiab': 0.0, 'PriorityLiab': 0.60},
    '指定以公允价值记账之金融负债': {'codes': ['004020012', '001002013', '002002010', '003015012'], 'TotalLiab': 0.0, 'PriorityLiab': 0.60},
    '衍生金融工具-负债': {'codes': ['004020013', '001002014', '002002011', '003015013'], 'TotalLiab': 0.0, 'PriorityLiab': 0.60},

    '预收账款(非流动)': {'codes': ['004020014', '003015014'], 'TotalLiab': 0.0, 'PriorityLiab': 0.05},
    '财务担保合同及负债(非流动)': {'codes': ['004020015'], 'TotalLiab': 0.0, 'PriorityLiab': 0.55},
    '职工薪酬及福利(非流动)': {'codes': ['004020016', '003015015'], 'TotalLiab': 0.0, 'PriorityLiab': 0.40},
    '预计负债': {'codes': ['004020017', '001002021', '002002017'], 'TotalLiab': 0.0, 'PriorityLiab': 0.60},
    '应付票据(非流动)': {'codes': ['004020018', '003015016'], 'TotalLiab': 0.0, 'PriorityLiab': 0.35},
    '其他金融负债(非流动)': {'codes': ['004020019'], 'TotalLiab': 0.0, 'PriorityLiab': 0.50},
    '可转换可赎回优先股': {'codes': ['004020020'], 'TotalLiab': 0.0, 'PriorityLiab': 0.60},

    # 汇总
    # '非流动负债其他项目': {'codes': ['004020997', '003015997'], 'TotalLiab': 0.0, 'PriorityLiab': 0.00},
    # '非流动负债合计': {'codes': ['004020999', '003015999'], 'TotalLiab': 0.0, 'PriorityLiab': 0.00},
    # '负债其他项目': {'codes': ['004021001', '001002997', '002002997', '003017001'], 'TotalLiab': 0.0, 'PriorityLiab': 0.00},

    # ---- 总负债（Total口径只用这个）----
    '总负债': {'codes': ['004025999', '001002999', '002002999', '003019999'], 'TotalLiab': 1.0, 'PriorityLiab': 0.00},



    # ------------------------
    # 下面是非004、金融行业常见“负债/义务”增量
    # （只增量追加到 DEFAULT_LIAB_WEIGHTS 末尾）
    # ------------------------

    # 1) 银行/券商：核心融资型负债（通常刚性、现金压力高）
    '客户存款': {'codes': ['001002001'], 'TotalLiab': 0.0, 'PriorityLiab': 1.00},                 # 存款挤兑/流动性核心
    '存款': {'codes': ['003001028'], 'TotalLiab': 0.0, 'PriorityLiab': 1.00},                     # 银行吸收存款
    '客户存款及保证金': {'codes': ['002002018'], 'TotalLiab': 0.0, 'PriorityLiab': 0.95},          # 含保证金，部分可波动但总体刚性
    '已发行存款证': {'codes': ['001002004'], 'TotalLiab': 0.0, 'PriorityLiab': 0.95},              # 有息、到期兑付刚性强
    '银行同业及其他金融机构存款(负债)': {'codes': ['001002002'], 'TotalLiab': 0.0, 'PriorityLiab': 0.95},  # 同业资金敏感、抽贷风险高
    '拆入资金': {'codes': ['001002011', '003007021'], 'TotalLiab': 0.0, 'PriorityLiab': 0.95},     # 同业拆借，期限短、滚动压力大
    '卖出回购金融资产': {'codes': ['001002020', '002002013'], 'TotalLiab': 0.0, 'PriorityLiab': 0.95},  # repo，本质融资
    '卖出回购金融资产款': {'codes': ['003007019'], 'TotalLiab': 0.0, 'PriorityLiab': 0.95},         # 同上口径差异
    '抵押担保融资': {'codes': ['001002012'], 'TotalLiab': 0.0, 'PriorityLiab': 0.90},              # 抵押融资/质押融资，条款驱动可能触发补担保
    '向中央银行借款': {'codes': ['001002028'], 'TotalLiab': 0.0, 'PriorityLiab': 0.90},            # 视为有息负债，通常刚性
    '银行贷款及透支': {'codes': ['003007010'], 'TotalLiab': 0.0, 'PriorityLiab': 1.00},            # 借款+透支，现金压力最高
    '借款': {'codes': ['001002019', '002002008'], 'TotalLiab': 0.0, 'PriorityLiab': 1.00},          # 有息负债核心
    '长期银行贷款': {'codes': ['003015001'], 'TotalLiab': 0.0, 'PriorityLiab': 0.90},              # 非流动但仍是有息刚性
    '其他帐项及准备': {'codes': ['001002008'], 'TotalLiab': 0.0, 'PriorityLiab': 0.60},             # 偏黑箱桶，可能夹带应付/融资/准备

    # 2) 债券/次级资本/保险负债：一般也刚性较强（但期限长，现金紧迫性略低于短融）
    '已发行债券': {'codes': ['001002009', '003007018'], 'TotalLiab': 0.0, 'PriorityLiab': 0.95},   # 有息、到期兑付
    '后偿负债': {'codes': ['001002016'], 'TotalLiab': 0.0, 'PriorityLiab': 0.75},                  # 次级/后偿，压力情景下可能延期但仍是负债
    '可转换票据及债券(流动)': {'codes': ['003007022'], 'TotalLiab': 0.0, 'PriorityLiab': 0.90},    # 流动端更紧迫
    '保险合同负债': {'codes': ['002002001'], 'TotalLiab': 0.0, 'PriorityLiab': 0.80},              # 赔付/退保等现金流压力（与业务结构强相关）
    '保险负债': {'codes': ['001002018'], 'TotalLiab': 0.0, 'PriorityLiab': 0.80},                   # 泛化保险负债口径
    '保险准备金': {'codes': ['002002012'], 'TotalLiab': 0.0, 'PriorityLiab': 0.70},                 # 偏估计项+监管约束，现金紧迫性中高
    '预收保费': {'codes': ['002002016'], 'TotalLiab': 0.0, 'PriorityLiab': 0.35},                   # 更像合同负债/履约义务，不是马上还钱
    '投资合同': {'codes': ['002002005'], 'TotalLiab': 0.0, 'PriorityLiab': 0.85},                   # 往往更“像负债”，可能有保证收益/赎回压力

    # 3) 券商/清算/客户资金：经常是“客户钱”，本质上需要随时兑付（高优先级）
    '代理买卖证券款': {'codes': ['003007020'], 'TotalLiab': 0.0, 'PriorityLiab': 0.90},            # 客户结算相关资金
    '客户信托存款': {'codes': ['003002020'], 'TotalLiab': 0.0, 'PriorityLiab': 0.90},              # 客户托管/信托，兑付/划转刚性强
    '保证金': {'codes': ['003002022'], 'TotalLiab': 0.0, 'PriorityLiab': 0.85},                     # 客户保证金/交易保证金
    '结算备付金': {'codes': ['003002023'], 'TotalLiab': 0.0, 'PriorityLiab': 0.85},                 # 清算所/交易所备付
    '客户垫款': {'codes': ['003002012'], 'TotalLiab': 0.0, 'PriorityLiab': 0.65},                   # 券商给客户的垫款，回收风险，但本身是资产；若你这列在负债端出现要谨慎
    '应付帐款及其他应付款': {'codes': ['001002017', '002002015'], 'TotalLiab': 0.0, 'PriorityLiab': 0.55},  # 典型黑箱桶，现金紧迫性中等
    '应付利息': {'codes': ['001002015'], 'TotalLiab': 0.0, 'PriorityLiab': 0.90},                  # 有息现金流刚性强
    '应付职工薪酬': {'codes': ['001002010'], 'TotalLiab': 0.0, 'PriorityLiab': 0.90},               # 刚性较强
    '应交税金': {'codes': ['002002003'], 'TotalLiab': 0.0, 'PriorityLiab': 0.98},                  # 强制性
    '应交税项': {'codes': ['001002005'], 'TotalLiab': 0.0, 'PriorityLiab': 0.98},                  # 强制性
    '应付保险给付和赔付': {'codes': ['002002002'], 'TotalLiab': 0.0, 'PriorityLiab': 0.90},         # 已发生赔付应付，现金刚性强
    '应付分保账款': {'codes': ['002002004'], 'TotalLiab': 0.0, 'PriorityLiab': 0.75},               # 再保险结算相关，较刚性

    # 4) 你列表里的“其他负债”（非004）——黑箱程度高，优先级中高（建议配合 OpaqueLiab 单独惩罚）
    '其他负债': {'codes': ['001002022', '002002009'], 'TotalLiab': 0.0, 'PriorityLiab': 0.60},

    # 5) 香港金管局/纸币相关：更像“发行/清算负债”，一般属于银行体系刚性义务
    '香港特区政府流通纸币': {'codes': ['001002003'], 'TotalLiab': 0.0, 'PriorityLiab': 0.90},
    '香港特区政府负债证明书': {'codes': ['001001006'], 'TotalLiab': 0.0, 'PriorityLiab': 0.90},
}



DEFAULT_FCF_WEIGHTS = {
    # -----------------------------
    # 1) 经营现金：基础项
    # -----------------------------
    "CFO_net": {
        "codes": ["003999"],
        "sign_policy": "as_is",
        "NormalFCF": 1.0,
        "ConservativeFCF": 1.0,
        "note": "经营业务现金净额，作为两种口径的起点",
    },

    # -----------------------------
    # 2) 保守口径：剔除非主营/不可持续的经营现金流入
    #    (若你的数据源里这些行存在，003999 通常“包含”它们，故用 -1 抵消)
    # -----------------------------
    "InterestReceived_Operating": {
        "codes": ["003001"],
        "sign_policy": "as_is",
        "NormalFCF": 0.0,
        "ConservativeFCF": -1.0,
        "note": "主营不应依赖利息收入（若是金融机构需改规则）",
    },
    "DividendReceived_Operating": {
        "codes": ["003004"],
        "sign_policy": "as_is",
        "NormalFCF": 0.0,
        "ConservativeFCF": -1.0,
        "note": "经营项下股息流入，通常非主营可持续现金",
    },
    "OperatingOtherItems": {
        "codes": ["003997"],
        "sign_policy": "as_is",
        "NormalFCF": 0.0,
        "ConservativeFCF": -1.0,
        "note": "经营其他项目：默认保守剔除（你也可改为 -0.5）",
    },
    "OperatingBalanceItems": {
        "codes": ["003998"],
        "sign_policy": "as_is",
        "NormalFCF": 0.0,
        "ConservativeFCF": -1.0,
        "note": "经营平衡项目：默认保守剔除",
    },

    # -----------------------------
    # 3) 维持性资本开支（CAPEX）：两种口径都扣
    # -----------------------------
    "Capex_PPE": {
        "codes": ["005005"],
        "sign_policy": "outflow_abs",
        "NormalFCF": 1.0,          # 注意：这里系数=+1，但 sign_policy 已经把它变成负数流出
        "ConservativeFCF": 1.0,
        "note": "购建固定资产：视为维持性资本开支（可按行业再拆维护/扩张）",
    },
    "Capex_Intangibles": {
        "codes": ["005007"],
        "sign_policy": "outflow_abs",
        "NormalFCF": 1.0,
        "ConservativeFCF": 1.0,
        "note": "购建无形资产及其他资产：通常也应计入CAPEX",
    },

    # -----------------------------
    # 4) 处置资产回收：正常口径可全额抵减，保守口径强折扣（非经常性）
    # -----------------------------
    "Proceeds_Disposal_PPE": {
        "codes": ["005004"],
        "sign_policy": "inflow_abs",
        "NormalFCF": 1.0,
        "ConservativeFCF": 0.3,
        "note": "处置固定资产回收现金：保守仅给 30% credit（可调）",
    },
    "Proceeds_Disposal_Intangibles": {
        "codes": ["005006"],
        "sign_policy": "inflow_abs",
        "NormalFCF": 1.0,
        "ConservativeFCF": 0.3,
        "note": "处置无形资产及其他资产：同上",
    },

    # -----------------------------
    # 5) 投资性投资：正常口径通常不纳入“经营FCF”（系数=0）
    #    保守口径：流入强折扣；流出按 0.5~1.0 扣（你想多保守就多扣）
    # -----------------------------
    "InvestmentRecovery": {
        "codes": ["005010"],
        "sign_policy": "inflow_abs",
        "NormalFCF": 0.0,
        "ConservativeFCF": 0.2,
        "note": "收回投资所得现金：保守只给 20% credit（不可持续/不确定）",
    },
    "InvestmentPayment": {
        "codes": ["005011"],
        "sign_policy": "outflow_abs",
        "NormalFCF": 0.0,
        "ConservativeFCF": 0.5,
        "note": "投资支付现金：保守扣 50%（你若要更严可设 1.0）",
    },
    "InterestReceived_Investing": {
        "codes": ["005001"],
        "sign_policy": "inflow_abs",
        "NormalFCF": 0.0,
        "ConservativeFCF": 0.2,
        "note": "投资已收利息：保守强折扣",
    },
    "DividendReceived_Investing": {
        "codes": ["005002"],
        "sign_policy": "inflow_abs",
        "NormalFCF": 0.0,
        "ConservativeFCF": 0.2,
        "note": "投资已收股息：保守强折扣",
    },

    # -----------------------------
    # 6) 并购/出售子公司：高度非经常性
    #    你的思路里属于“投资性投资”，一般不作为主营收入；
    #    但现金流出是真实消耗，保守可部分扣除（或全扣）
    # -----------------------------
    "AcquireSubsidiary": {
        "codes": ["005009"],
        "sign_policy": "outflow_abs",
        "NormalFCF": 0.0,
        "ConservativeFCF": 0.5,
        "note": "收购附属公司：保守扣 50%（更严可 1.0）",
    },
    "DisposeSubsidiary": {
        "codes": ["005008"],
        "sign_policy": "inflow_abs",
        "NormalFCF": 0.0,
        "ConservativeFCF": 0.0,
        "note": "出售附属公司：保守不给 credit（一次性）",
    },

    # -----------------------------
    # 7) 资金形态变动：你明确要忽略 -> 系数=0
    # -----------------------------
    "DepositChange": {
        "codes": ["005003", "002014"],
        "sign_policy": "as_is",
        "NormalFCF": 0.0,
        "ConservativeFCF": 0.0,
        "note": "存款(减少/增加)：视为现金管理形态变动",
    },
    "FinancingFlows_AllIgnored": {
        "codes": ["007001", "007002", "007003", "007004", "007006", "007007", "007008", "007009", "007010", "007011", "007012", "007013", "007014"],
        "sign_policy": "ignore",
        "NormalFCF": 0.0,
        "ConservativeFCF": 0.0,
        "note": "借款/还款/股息/发债/租赁/受限现金变动等：不用于FCF",
    },
}

