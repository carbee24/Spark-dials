import pandas as pd
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from hbase.Hbase import Client, Mutation

def log_file_process(logfile):
    start_line_num = 0
    end_line_num = 0
    summary_list = None
    space_group = None

    with open(logfile, 'r') as f:
        line_num = 0
        # print(f.readline())
        # print(f.readline())
        # print(f.readlines())
        for line in f.readlines():
            # print(line)
            str = 'Saving reindexed experiments to symmetrized.expt in space group'
            if str in line:
                line = line[len(str):]
                line.replace('\n', '').strip()
                space_group_list = line.split()
                space_group = ' '.join(space_group_list)
            if 'Summary of merging statistics' in line:
                start_line_num = line_num
            if 'Writing html report to dials.scale.html' in line:
                end_line_num = line_num
                break
            line_num += 1
            # print(line_num)
        # print(start_line_num, end_line_num)
        # print(line_num)
        # print(f.readlines())
        f.seek(0)
        summary_list = f.readlines()[start_line_num: end_line_num]
        f.close()

    # summary_list = None
    # with open('spark_dials_pipeline.log', 'r') as f :
    #     summary_list = f.readlines()[start_line_num: end_line_num]

    # print(summary_list)
    # for i in summary_list:
    #     print(i)
    # with open('summary.txt', 'w') as f:
    #     for line in summary_list[3:]:
    #         f.write(line)

    #     f.close()

    summary_list = summary_list [2:]
    summary_list_new = []
    for i in summary_list:
        summary_list_new.append(i[:-1])
    Overall_col = []
    Low_col = []
    High_col = []
    diffraction_indexs_row = []
    for i in summary_list_new:
        # print(i)
        line_list = []
        if i[:31].rstrip() != '':
            line_list.append(i[:31].rstrip())
        # if i[31:].split() != []:

        line_list = line_list + i[31:].split()
        # print(len(line_list))
        if len(line_list) == 0 or len(line_list) == 3:
            continue
        elif len(line_list) == 2:
            diffraction_indexs_row.append(line_list[0] if '/' not in line_list[0] else line_list[0].replace('/', '|'))
            Overall_col.append(line_list[1])
            Low_col.append('null value')
            High_col.append('null value')
        else:
            diffraction_indexs_row.append(line_list[0] if '/' not in line_list[0] else line_list[0].replace('/', '|'))
            Overall_col.append(line_list[1])
            Low_col.append(line_list[2])
            High_col.append(line_list[3])

    # print(len(diffraction_indexs_row))
    # print(len(Overall_col))
    # print(len(Low_col))
    # print(len(Low_col))
    assert len(diffraction_indexs_row) == len(Overall_col)
    assert len(diffraction_indexs_row) == len(Low_col)
    assert len(diffraction_indexs_row) == len(High_col)

    data = {
        "Overall" : Overall_col,
        "Low"     : Low_col,
        "High"    : High_col
    }

    df = pd.DataFrame(data, index = diffraction_indexs_row)

    # print(df)
    # print(space_group)

    return space_group, df


#
# client = Client(protocol)
#
# # col1 = ColumnDescriptor(name='merge_stat')
# # client.createTable('5-5-1_1', [col1])
# for row in df.index.tolist():
#     for col in df.columns.tolist():
#         print(row, col, df.loc[row, col])
#         value = df.loc[row, col]
#         column = 'merge_stat:' + col
#         mutation = [Mutation(column=column, value = value)]
#         client.mutateRow('5-5-1_1', row, mutation)

result_dict = {
    'Anomalous completeness': {
        'High': None,
        'Low': None,
        'Overall': None
    },
    'Anomalous correlation': {
        'High': None,
        'Low': None,
        'Overall': None
    },
    'Anomalous multiplicity': {
        'High': None,
        'Low': None,
        'Overall': None
    },
    'Anomalous slope': {
        'High': None,
        'Low': None,
        'Overall': None
    },
    'CC half': {
        'High': None,
        'Low': None,
        'Overall': None
    },
    'Completeness': {
        'High': None,
        'Low': None,
        'Overall': None
    },
    'High resolution limit': {
        'High': None,
        'Low': None,
        'Overall': None
    },
    'I|sigma': {
        'High': None,
        'Low': None,
        'Overall': None
    },
    'Low resolution limit': {
        'High': None,
        'Low': None,
        'Overall': None
    },
    'Multiplicity': {
        'High': None,
        'Low': None,
        'Overall': None
    },
    'Rmeas(I)': {
        'High': None,
        'Low': None,
        'Overall': None
    },
    'Rmeas(I+|-)': {
        'High': None,
        'Low': None,
        'Overall': None
    },
    'Rmerge(I)': {
        'High': None,
        'Low': None,
        'Overall': None
    },
    'Rmerge(I+|-)': {
        'High': None,
        'Low': None,
        'Overall': None
    },
    'Rpim(I)': {
        'High': None,
        'Low': None,
        'Overall': None
    },
    'Rpim(I+|-)': {
        'High': None,
        'Low': None,
        'Overall': None
    },
    'Total observations': {
        'High': None,
        'Low': None,
        'Overall': None
    },
    'Total unique': {
        'High': None,
        'Low': None,
        'Overall': None
    },
    'dF|F': {
        'High': None,
        'Low': None,
        'Overall': None
    },
    'dI|s(dI)': {
        'High': None,
        'Low': None,
        'Overall': None
    },
    'Space info': {
        'space group': None,
    }
}
class HBaseClient(Client):
    def __init__(self, protocol):
        super(HBaseClient, self).__init__(protocol)

    def extract_to_hbase(self, dataset_name):
        space_group, df = log_file_process("/home/zhangding/local/test_data/spark_pipeline/spark_dials_pipeline_5min.log")
        column = 'Space info:space group'
        mutation = [Mutation(column=column, value=space_group)]
        self.mutateRow('Merge_Statistics', dataset_name, mutation)
        for row in df.index.tolist():
            for col in df.columns.tolist():
                print(row, col, df.loc[row, col])
                value = df.loc[row, col]
                column = row + ':' + col
                mutation = [Mutation(column=column, value = value)]
                self.mutateRow('Merge_Statistics', dataset_name, mutation)

    def get_data(self, dataset_name, col_family = None):
        result = self.getRow('Merge_Statistics', dataset_name)[0]
        if col_family:
            single_result_dict = {}
            for key, item in result.columns.items():
                family, col = key.split(':')
                if col_family == family:
                    single_result_dict[col_family] = result_dict[family]
                    single_result_dict[col_family][col] =  item.value
            if not single_result_dict:
                return {"status": "no %s col family exists"%col_family}
            return single_result_dict

        for key, item in result.columns.items():
            col_family, col = key.split(':')
            print(col_family, col)
            result_dict_copy = result_dict.copy()
            result_dict_copy[col_family][col] = item.value

        return result_dict_copy




if __name__ == "__main__":
    log_file_process("/home/zhangding/local/test_data/spark_pipeline/pipeline_test/cbf/spark_dials_pipeline.log")
        