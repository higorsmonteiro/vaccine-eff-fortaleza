'''

'''

class JOIN_TESTER:
    def __init__(self):
        pass

    def vacinejaplus_obitos(self, vacinejaplus_df, obitos_df, variables_hash, datehash_left, datehash_right):
        '''
            Join the two tables based on the variables parsed in the 'variables_hash'.
            The goal of this function is to perform several forms of join operations
            between the vaccine records and deaths by covid records. Since there isn't
            any unique variable, we need to perform join using several variables, but 
            using the minimum number of variables to avoid losing many records.

            Args:
                vacinejaplus_df: 
                obitos_df:
                variables_hash:
                date_hash:
            Return:
                tb_list:
        '''
        items_col = variables_hash.items()
        col_left = [ x[0] for x in items_col ]
        col_right = [ x[1] for x in items_col ]

        vacinejaplus_df["COLCOMP"] = vacinejaplus_df[col_left].apply(lambda row: join_columns(row, col_left, datehash_left), axis=1)
        obitos_df["COLCOMP"] = obitos_df[col_right].apply(lambda row: join_columns(row, col_right, datehash_right), axis=1)
        vacinejaplus_df = vacinejaplus_df.drop_duplicates(subset=["COLCOMP"])
        obitos_df = obitos_df.drop_duplicates(subset=["COLCOMP"])

        joined_df = obitos_df.merge(vacinejaplus_df, on="COLCOMP", how="inner")
        joined_df = joined_df.drop_duplicates(subset=["cpf"], keep="first")
        return joined_df

    def vacinejaplus_hospital(self, vacinejaplus_df, hospital_df, variables_hash, date_hash):
        '''
        
        '''
        items = variables_hash.items()
        col_left = [ x[0] for x in items ]
        col_right = [ x[1] for x in items ]
        pass

    def vacinejaplus_tests(self, vacinejaplus_df, tests_df, variables_hash, date_hash):
        '''
        
        '''
        items = variables_hash.items()
        col_left = [ x[0] for x in items ]
        col_right = [ x[1] for x in items ]
        pass


def join_columns(row, colnames, date_hash):
    '''
        Create a single string by concatenating all columns' values parsed.
    '''
    final_str = ""
    for j in range(len(colnames)):
        col_name = colnames[j]
        is_date = date_hash[col_name]
        if is_date:
            final_str += str(row[col_name].date())
        else:
            final_str += str(row[col_name])
    return final_str
